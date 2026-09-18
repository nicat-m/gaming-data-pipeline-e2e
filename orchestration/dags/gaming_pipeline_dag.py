"""
Phase 0 skeleton DAG for the Gaming Lakehouse pipeline.

Defines the shape of the pipeline so the orchestration layer is provable
end-to-end from day one:

    load_raw_to_postgres --> dbt_run_staging --> dbt_run_curated
                                              \-> spark_batch_daily_engagement

Note the generator and Kafka are NOT orchestrated by this DAG: events flow
generator -> source Postgres -> Debezium (Kafka Connect, CDC) -> Kafka ->
consumer -> RustFS continuously and independently. This DAG only owns the
batch steps from RustFS onward. Task bodies are placeholders on purpose -
no business logic ships until Phase 1 (ingestion/storage), Phase 2
(transformation), and Phase 3 (Spark aggregation), per
docs/PROJECT_PLAN.md.

The DAG is created paused (AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION) so
it never runs unattended before its task logic is implemented.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "gaming-lakehouse",
    "retries": 2,
}


def load_raw_to_postgres(**_context) -> None:
    """
    Phase 1 will implement: list new objects under raw/game_events/ in the
    RustFS bucket and COPY them into Postgres schema `raw`, using the
    offset-range in the object key to skip files already loaded.
    """
    raise NotImplementedError("Implemented in Phase 1 - Ingestion & Storage.")


with DAG(
    dag_id="gaming_pipeline",
    description=(
        "Postgres(source) -> Debezium/Kafka -> RustFS -> Postgres(raw) "
        "-> dbt staging/curated + Spark batch aggregation"
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["gaming", "phase-0-skeleton"],
) as dag:

    load_raw = PythonOperator(
        task_id="load_raw_to_postgres",
        python_callable=load_raw_to_postgres,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            "cd /opt/airflow/transformation/dbt_gaming && "
            "DBT_PROFILES_DIR=. dbt run --select staging"
        ),
    )

    dbt_run_curated = BashOperator(
        task_id="dbt_run_curated",
        bash_command=(
            "cd /opt/airflow/transformation/dbt_gaming && "
            "DBT_PROFILES_DIR=. dbt run --select curated"
        ),
    )

    spark_batch_daily_engagement = BashOperator(
        task_id="spark_batch_daily_engagement",
        bash_command=(
            "spark-submit --master {{ var.value.get('spark_master_url', 'spark://spark-master:7077') }} "
            "/opt/airflow/transformation/spark_jobs/daily_engagement_batch.py "
            "--date {{ ds }}"
        ),
    )

    load_raw >> dbt_run_staging >> dbt_run_curated
    load_raw >> spark_batch_daily_engagement
