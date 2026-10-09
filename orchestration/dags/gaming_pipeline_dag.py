"""
Gaming Lakehouse batch pipeline - Phase 1 walking skeleton.

    land_raw_kafka_to_rustfs            (ingestion: Kafka -> RustFS raw zone)
        -> load_raw_to_clickhouse       (raw: RustFS -> ClickHouse raw database)
        -> dbt_run_staging              (staging: typed/cleaned 1:1 models)
        -> dbt_test_staging             (DQ checks - failure blocks downstream)
        -> dbt_run_curated              (curated: dim_player, fct_in_game_transaction)
        -> publish_serving              (serving: SQL view + CSV report on RustFS)

    spark_connectivity_proof            (parallel: proves the Spark cluster is
                                         integrated; the real S3A batch job is
                                         Phase 3, see docs/decisions/0001-*.md)

The generator and CDC are NOT orchestrated here: events flow
generator -> source Postgres -> Debezium/Kafka Connect -> Kafka continuously
and independently. This DAG owns the batch slice from the Kafka topic onward
and is parameterised by logical date ({{ ds }}), so a reviewer can run it for
a specific day (see README "Run the pipeline").

Deliberate failure switch: set PIPELINE_FAIL_SWITCH=1 in .env, `make up` to
recreate the containers, then trigger the DAG - the ingestion task raises
AirflowFailException, the run is marked failed and every downstream task is
skipped. Remove the variable (or set 0) and re-trigger to go back to normal.

Retries: the ingestion task retries up to 2 times with a 1-minute delay;
all other tasks retry twice (see default_args).
"""
from __future__ import annotations

import csv
import io
import json
import os
import uuid
from datetime import datetime, timedelta

import clickhouse_connect
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from config.settings import settings

RAW_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS raw.game_events
(
    event_id              UUID,
    event_type            String,
    event_time_raw        String,
    session_id            Nullable(UUID),
    amount_usd            Nullable(Float64),
    currency              Nullable(String),
    player_id             Nullable(UUID),
    player_display_name   Nullable(String),
    player_country        Nullable(String),
    player_signup_date    Nullable(String),  # ISO date from Debezium epoch-days
    device_id             UUID,
    device_platform       String,
    device_os_version     Nullable(String),
    game_id               String,
    game_name             Nullable(String),
    game_genre            Nullable(String),
    extra                 String DEFAULT '{}',
    source_partition_date String,  # partition key dt=<YYYY-MM-DD> (keeps the raw layer dumb)
    loaded_at             DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree
ORDER BY (source_partition_date, event_id)
"""


RAW_COLUMNS = [
    "event_id", "event_type", "event_time_raw", "session_id", "amount_usd",
    "currency", "player_id", "player_display_name", "player_country",
    "player_signup_date", "device_id", "device_platform", "device_os_version",
    "game_id", "game_name", "game_genre", "extra",
]

default_args = {
    "owner": "gaming-lakehouse",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def _print(task: str, msg: str) -> None:
    print(f"[{task}] {msg}")


def land_raw_kafka_to_rustfs(partition_date: str, **_context) -> dict:
    """Ingestion task: consume available Kafka messages into the raw zone.

    Idempotency: objects are keyed raw/game_events/dt=<ds>/<first>-<last>.jsonl
    with the Kafka offset range in the name, so a re-run for the same logical
    date overwrites the same objects instead of duplicating data.
    """
    task = "land_raw_kafka_to_rustfs"
    if os.getenv("PIPELINE_FAIL_SWITCH", "0") == "1":
        raise AirflowFailException(
            "PIPELINE_FAIL_SWITCH=1 - deliberate failure triggered. "
            "All downstream tasks are skipped. Remove the switch and re-trigger."
        )

    from ingestion.consumer.kafka_to_rustfs import run as consumer_run

    _print(task, f"start: input=kafka topic={settings.kafka.topic_game_events} "
                 f"output=s3://{settings.rustfs.raw_bucket}/raw/game_events/dt={partition_date}/")
    summary = consumer_run(
        batch_size=200,
        max_batches=25,          # bounded run: at most 5000 messages per run
        partition_date=partition_date,
        idle_exit_seconds=20,    # end the run when the broker has no new events
    )
    _print(task, f"end: rows_read={summary['messages_read']} "
                 f"batches_written={summary['batches_written']}")
    return summary


def _coerce_raw_value(column: str, value):
    """Map Debezium JSON payload values to the raw column types.

    Raw-layer philosophy: land the payload as it arrived - no typing here
    (that is the dbt staging layer's job). Only the minimum is done to fit
    the raw table's column types:

    - UUID columns accept uuid objects (Debezium JSON mode sends strings).
    - Debezium DATE (player_signup_date) arrives as epoch-days int; stored
      as-is (string) and cast to a real Date in staging.
    - Numeric values (amount_usd) pass through as floats for Float64.
    """
    if value is None:
        return None
    if column in ("event_id", "player_id", "device_id", "session_id"):
        try:
            return uuid.UUID(str(value))
        except ValueError:
            return None  # malformed id: keep the row, null the id
    if column == "player_signup_date":
        return str(value)  # epoch-days int as received; typed in staging
    return value


def load_raw_to_clickhouse(partition_date: str, **_context) -> int:
    """Raw-load task: land every NDJSON object for the logical date in the
    ClickHouse raw database.

    Idempotency: ReplacingMergeTree + the same (source_partition_date,
    event_id) ORDER BY key make re-runs converge to one row per event - a
    re-run's insert collapses on the next merge (or is deduped at read time
    with FINAL / argMax in the curated models).
    """
    task = "load_raw_to_clickhouse"
    prefix = f"raw/game_events/dt={partition_date}/"
    s3 = _get_s3_client()
    objects = s3.list_objects_v2(Bucket=settings.rustfs.raw_bucket, Prefix=prefix).get("Contents", [])
    _print(task, f"start: input=s3://{settings.rustfs.raw_bucket}/{prefix} "
                 f"({len(objects)} objects) output={settings.clickhouse.host}:"
                 f"{settings.clickhouse.port} raw.game_events")

    rows_read = 0
    rows_inserted = 0
    client = _get_clickhouse_client()
    client.command(RAW_TABLE_DDL)
    _print(task, "raw.game_events table ready (ReplacingMergeTree)")

    for obj in objects:
        body = s3.get_object(Bucket=settings.rustfs.raw_bucket, Key=obj["Key"])
        records = [json.loads(line) for line in body["Body"].read().splitlines() if line.strip()]
        rows_read += len(records)
        batch = []
        for rec in records:
            row = rec.get("after") if isinstance(rec.get("after"), dict) else rec
            values = [_coerce_raw_value(col, row.get(col)) for col in RAW_COLUMNS]
            batch.append((*values, partition_date))  # raw layer: dt=<date> stays a string
        if batch:
            client.insert(
                "raw.game_events",
                batch,
                column_names=[*RAW_COLUMNS, "source_partition_date"],
            )
            rows_inserted += len(batch)
    _print(task, f"end: rows_read={rows_read} rows_inserted={rows_inserted} "
                 f"(ReplacingMergeTree dedupes re-runs on the same event_id)")
    return rows_inserted


def spark_connectivity_proof(**_context) -> None:
    """Connectivity proof for the Spark component (Phase 1): run a trivial
    spark-submit inside the Spark container. The real S3A batch job is
    Phase 3 - docs/decisions/0001-spark-batch-s3a-phase3.md.
    """
    import docker

    task = "spark_connectivity_proof"
    _print(task, "start: docker exec gaming-spark-master spark-submit connectivity_proof.py")
    client = docker.from_env()
    # HOME must point at a writable dir: the Bitnami Spark image resolves the
    # ivy cache under $HOME/.ivy2 and fails with "basedir must be absolute: ?/.ivy2/local"
    # when HOME is unset/"/" (as it is for a bare exec without the image's entrypoint).
    result = client.containers.get("gaming-spark-master").exec_run(
        cmd=[
            "sh", "-c",
            "export HOME=/opt/bitnami/spark; "
            "spark-submit --master local[1] /opt/spark-apps/connectivity_proof.py",
        ],
    )
    output = result.output.decode(errors="replace")
    print(output[-2000:])
    if result.exit_code != 0:
        raise AirflowFailException(f"Spark connectivity proof failed (exit={result.exit_code})")
    _print(task, "end: spark-submit succeeded")


def publish_serving(partition_date: str, **_context) -> None:
    """Serving task: expose one curated object for consumers - a SQL view
    (`serving.v_player_engagement_daily`, queryable by Superset / any SQL
    client) and a CSV report exported to RustFS under serving/.

    Idempotency: CREATE OR REPLACE VIEW + deterministic report object key
    (dt=<ds>/report.csv) make re-runs overwrite instead of duplicate.
    """
    task = "publish_serving"
    view_sql = """
    CREATE OR REPLACE VIEW serving.v_player_engagement_daily AS
    SELECT
        f.player_id              AS player_id,
        p.country                AS country,
        count(f.event_id)        AS transactions,
        sum(f.amount_usd)        AS total_amount_usd,
        max(f.event_time)        AS last_txn_at
    FROM curated.fct_in_game_transaction f
    JOIN curated.dim_player p USING (player_id)
    GROUP BY f.player_id, p.country
    """
    client = _get_clickhouse_client()
    client.command("CREATE DATABASE IF NOT EXISTS serving")
    client.command(view_sql)
    rows = client.query("SELECT * FROM serving.v_player_engagement_daily")
    columns = rows.column_names

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    writer.writerows(rows.result_rows)
    key = f"serving/player_engagement_daily/dt={partition_date}/report.csv"
    s3 = _get_s3_client()
    s3.put_object(Bucket=settings.rustfs.raw_bucket, Key=key, Body=buf.getvalue().encode(), ContentType="text/csv")
    _print(task, f"start: input=curated database output=serving.v_player_engagement_daily "
                 f"+ s3://{settings.rustfs.raw_bucket}/{key}")
    _print(task, f"end: rows_published={len(rows.result_rows)}")


def _get_clickhouse_client():
    # Warehouse client used by the raw loader and the serving publisher.
    # Inside the compose network the service name is `clickhouse`; the
    # settings default (localhost) only matters for host-side CLI use.
    host = settings.clickhouse.host
    return clickhouse_connect.get_client(
        host="clickhouse" if host in ("localhost", "127.0.0.1") else host,
        port=settings.clickhouse.port,
        username=settings.clickhouse.user,
        password=settings.clickhouse.password,
        database=settings.clickhouse.db,
    )


def _get_s3_client():
    # Reuse the consumer's boto3 client builder - one place configures RustFS.
    from ingestion.consumer.kafka_to_rustfs import _get_s3_client as build_client

    return build_client()


with DAG(
    dag_id="gaming_pipeline",
    description=(
        "Postgres(source) -> Debezium/Kafka -> RustFS(raw) -> Postgres(raw) "
        "-> dbt staging/curated -> serving view + CSV report"
    ),
    start_date=datetime(2026, 1, 1),
    schedule="@daily",          # declared schedule; DAG ships paused
    catchup=False,
    default_args=default_args,
    tags=["gaming", "phase-1"],
) as dag:

    land_raw = PythonOperator(
        task_id="land_raw_kafka_to_rustfs",
        python_callable=land_raw_kafka_to_rustfs,
        op_kwargs={"partition_date": "{{ ds }}"},   # logical-date parameter
    )

    load_raw = PythonOperator(
        task_id="load_raw_to_clickhouse",
        python_callable=load_raw_to_clickhouse,
        op_kwargs={"partition_date": "{{ ds }}"},
    )

    spark_proof = PythonOperator(
        task_id="spark_connectivity_proof",
        python_callable=spark_connectivity_proof,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            "cd /opt/airflow/transformation/dbt_gaming && "
            "DBT_PROFILES_DIR=. dbt run --select staging"
        ),
    )

    # DQ gate: not_null/unique/accepted_values on staging + a row-count
    # singular test. A failing check fails this task -> the curated and
    # serving tasks are skipped and the run is marked failed.
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            "cd /opt/airflow/transformation/dbt_gaming && "
            "DBT_PROFILES_DIR=. dbt test --select staging"
        ),
    )

    dbt_run_curated = BashOperator(
        task_id="dbt_run_curated",
        bash_command=(
            "cd /opt/airflow/transformation/dbt_gaming && "
            "DBT_PROFILES_DIR=. dbt run --select curated"
        ),
    )

    publish = PythonOperator(
        task_id="publish_serving",
        python_callable=publish_serving,
        op_kwargs={"partition_date": "{{ ds }}"},
    )

    land_raw >> load_raw >> dbt_run_staging >> dbt_test_staging >> dbt_run_curated >> publish
    spark_proof
