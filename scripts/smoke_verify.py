"""Phase 1 smoke-test verifier: row counts per layer + reconciliation.

Run via `make smoke` (scripts/smoke.sh) - uses the real services from
docker compose: the ClickHouse warehouse and RustFS (boto3), reached
through the same central config the pipeline uses.

Usage: PYTHONPATH=. .venv/bin/python scripts/smoke_verify.py YYYY-MM-DD
"""
from __future__ import annotations

import sys

import clickhouse_connect

from config.settings import settings


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: smoke_verify.py YYYY-MM-DD")
        return 2
    ds = sys.argv[1]

    client = clickhouse_connect.get_client(
        host=settings.clickhouse.host,
        port=settings.clickhouse.port,
        username=settings.clickhouse.user,
        password=settings.clickhouse.password,
        database=settings.clickhouse.db,
    )
    raw_count = client.query(
        f"SELECT count(*) FROM raw.game_events WHERE source_partition_date = '{ds}'"
    ).result_rows[0][0]
    raw_ds_distinct = client.query(
        f"SELECT uniq(event_id) FROM raw.game_events WHERE source_partition_date = '{ds}'"
    ).result_rows[0][0]
    raw_total = client.query("SELECT count(*) FROM raw.game_events").result_rows[0][0]
    raw_distinct = client.query("SELECT uniq(event_id) FROM raw.game_events").result_rows[0][0]
    # Every logical date loaded so far: proves different dates coexist.
    raw_by_date = client.query(
        "SELECT source_partition_date, count(*), uniq(event_id) FROM raw.game_events "
        "GROUP BY source_partition_date ORDER BY source_partition_date"
    ).result_rows
    staging_count = client.query("SELECT count(*) FROM staging.stg_game_events").result_rows[0][0]
    staging_ds = client.query(
        f"SELECT count(*) FROM staging.stg_game_events WHERE source_partition_date = '{ds}'"
    ).result_rows[0][0]
    staging_dirty = client.query(
        "SELECT count(*) FROM staging.stg_game_events WHERE event_time IS NULL"
    ).result_rows[0][0]
    fct_count = client.query("SELECT count(*) FROM curated.fct_in_game_transaction").result_rows[0][0]
    dim_count = client.query("SELECT count(*) FROM curated.dim_player").result_rows[0][0]
    # The consumer-facing interface: a plain SQL query against the serving
    # view (same as any BI tool / Superset chart would issue).
    serving_count = client.query("SELECT count(*) FROM serving.v_player_engagement_daily").result_rows[0][0]
    expected_fct = client.query(
        "SELECT count(*) FROM staging.stg_game_events "
        "WHERE event_type = 'in_game_transaction' AND event_time IS NOT NULL"
    ).result_rows[0][0]

    # --- second source: external games API -> raw.games_catalog -> stg_games -> dim_game
    games_raw_ds = client.query(
        f"SELECT count(*) FROM raw.games_catalog WHERE source_partition_date = '{ds}'"
    ).result_rows[0][0]
    games_latest_distinct = client.query(
        "SELECT uniq(game_id) FROM raw.games_catalog "
        "WHERE source_partition_date = (SELECT max(source_partition_date) FROM raw.games_catalog)"
    ).result_rows[0][0]
    games_stg = client.query("SELECT count(*) FROM staging.stg_games").result_rows[0][0]
    games_dim = client.query("SELECT count(*) FROM curated.dim_game").result_rows[0][0]
    fct_matched = client.query(
        "SELECT count(*) FROM curated.fct_in_game_transaction f "
        "INNER JOIN curated.dim_game g ON f.game_id = g.game_id"
    ).result_rows[0][0]
    game_revenue_rows = client.query("SELECT count(*) FROM serving.v_game_revenue").result_rows[0][0]

    from ingestion.consumer.kafka_to_rustfs import _get_s3_client

    s3 = _get_s3_client()
    report_key = f"serving/player_engagement_daily/dt={ds}/report.csv"
    listing = s3.list_objects_v2(
        Bucket=settings.rustfs.raw_bucket,
        Prefix=f"serving/player_engagement_daily/dt={ds}/",
    )
    report_exists = bool(listing.get("Contents"))

    print("layer                        | rows")
    print("-----------------------------+-----------")
    print(f"raw.game_events (ds={ds})  | {raw_count}")
    print(f"  distinct event_id (ds)    | {raw_ds_distinct}")
    print(f"raw.game_events (total)     | {raw_total}")
    print(f"  distinct event_id (total) | {raw_distinct}")
    print(f"staging.stg_game_events(ds) | {staging_ds}")
    print(f"staging.stg_game_events     | {staging_count}")
    print(f"  flagged dirty (NULL ts)   | {staging_dirty}")
    print(f"curated.fct_transactions    | {fct_count}")
    print(f"curated.dim_player          | {dim_count}")
    print(f"serving view rows           | {serving_count}")
    print(f"serving CSV report          | {report_key} exists={report_exists}")
    print(f"raw.games_catalog (ds={ds}) | {games_raw_ds}   (API source)")
    print(f"staging.stg_games           | {games_stg}")
    print(f"curated.dim_game            | {games_dim}")
    print(f"  fact rows matched to game | {fct_matched} of {fct_count} "
          f"(unmatched = events with ids outside the catalog)")
    print(f"serving v_game_revenue rows | {game_revenue_rows}")
    print("raw rows per logical date   | date: rows (distinct events)")
    for d, n, u in raw_by_date:
        print(f"  {d}                | {n} ({u})")

    failures: list[str] = []
    if raw_count < 1000:
        failures.append(f"raw has {raw_count} rows for ds={ds}; Phase 1 requires >= 1000")
    # Staging is a FINAL view over ReplacingMergeTree: it must equal raw's
    # DISTINCT events (pre-merge duplicates in raw are invisible downstream).
    # Any difference raw_count - raw_ds_distinct is re-run duplication that
    # ClickHouse has not merged yet - reported above, removed at read time.
    if raw_count != raw_ds_distinct:
        failures.append(
            f"raw has duplicates for ds={ds}: {raw_count} rows vs {raw_ds_distinct} distinct events"
        )
    if staging_ds != raw_ds_distinct:
        failures.append(
            f"staging ({staging_ds}) != raw distinct events ({raw_ds_distinct}) for ds={ds}"
        )
    if staging_count != raw_distinct:
        failures.append(f"staging total ({staging_count}) != raw distinct total ({raw_distinct})")
    if fct_count != expected_fct:
        failures.append(f"fct ({fct_count}) != expected ({expected_fct})")
    if games_raw_ds == 0:
        failures.append(f"raw.games_catalog has no rows for ds={ds} (games API ingestion)")
    if games_dim != games_latest_distinct or games_stg != games_dim:
        failures.append(
            f"games: raw distinct {games_latest_distinct} / staging {games_stg} / dim_game {games_dim} must match"
        )
    if fct_matched == 0:
        failures.append("no fact row joins to dim_game: game_id in events does not match the API catalog")
    if game_revenue_rows == 0:
        failures.append("serving.v_game_revenue is empty")
    if dim_count == 0:
        failures.append("dim_player is empty")
    if serving_count == 0:
        failures.append("serving view is empty")
    if not report_exists:
        failures.append(f"serving report object missing: {report_key}")

    if failures:
        print("\nRECONCILIATION FAILED:")
        for f in failures:
            print(f" - {f}")
        return 1
    print("\nreconciliation OK: staging is 1:1 with raw (deduped via FINAL); fact "
          "matches its source slice; serving view and CSV report are queryable/existent.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
