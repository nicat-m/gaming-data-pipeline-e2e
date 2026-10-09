"""Phase 1 smoke-test verifier: row counts per layer + reconciliation.

Run via `make smoke` (scripts/smoke.sh) - uses the real services from
docker compose: the warehouse Postgres (psycopg2) and RustFS (boto3),
reached through the same central config the pipeline uses.

Usage: PYTHONPATH=. .venv/bin/python scripts/smoke_verify.py YYYY-MM-DD
"""
from __future__ import annotations

import sys

import psycopg2

from config.settings import settings


def _count(cur, sql: str) -> int:
    cur.execute(sql)
    return int(cur.fetchone()[0])


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: smoke_verify.py YYYY-MM-DD")
        return 2
    ds = sys.argv[1]

    conn = psycopg2.connect(settings.postgres.dsn)
    try:
        with conn.cursor() as cur:
            raw_count = _count(cur, f"SELECT count(*) FROM raw.game_events WHERE source_partition_date = '{ds}'")
            raw_total = _count(cur, "SELECT count(*) FROM raw.game_events")
            staging_count = _count(cur, "SELECT count(*) FROM staging.stg_game_events")
            staging_dirty = _count(cur, "SELECT count(*) FROM staging.stg_game_events WHERE event_time IS NULL")
            fct_count = _count(cur, "SELECT count(*) FROM curated.fct_in_game_transaction")
            dim_count = _count(cur, "SELECT count(*) FROM curated.dim_player")
            # The consumer-facing interface: a plain SQL query against the
            # serving view (same as any BI tool / SQL client would issue).
            cur.execute("SELECT count(*) FROM serving.v_player_engagement_daily")
            serving_count = int(cur.fetchone()[0])
    finally:
        conn.close()

    from ingestion.consumer.kafka_to_rustfs import _get_s3_client

    s3 = _get_s3_client()
    report_key = f"serving/player_engagement_daily/dt={ds}/report.csv"
    listing = s3.list_objects_v2(
        Bucket=settings.rustfs.raw_bucket,
        Prefix=f"serving/player_engagement_daily/dt={ds}/",
    )
    report_exists = bool(listing.get("Contents"))

    print("layer                     | rows")
    print("--------------------------+-----------")
    print(f"raw.game_events (ds={ds}) | {raw_count}")
    print(f"raw.game_events (total)   | {raw_total}")
    print(f"staging.stg_game_events   | {staging_count}")
    print(f"  flagged dirty (NULL ts) | {staging_dirty}")
    print(f"curated.fct_transactions  | {fct_count}")
    print(f"curated.dim_player        | {dim_count}")
    print(f"serving view rows         | {serving_count}")
    print(f"serving CSV report        | {report_key} exists={report_exists}")

    failures: list[str] = []
    if raw_count < 1000:
        failures.append(f"raw has {raw_count} rows for ds={ds}; Phase 1 requires >= 1000")
    if staging_count != raw_total:
        failures.append(f"staging ({staging_count}) != raw total ({raw_total}); staging is a 1:1 view")
    # Reconciliation for the fact: transactions = staging rows of type
    # in_game_transaction minus malformed-timestamp rows (explained, not hidden).
    conn = psycopg2.connect(settings.postgres.dsn)
    try:
        with conn.cursor() as cur:
            expected_fct = _count(
                cur,
                "SELECT count(*) FROM staging.stg_game_events "
                "WHERE event_type = 'in_game_transaction' AND event_time IS NOT NULL",
            )
    finally:
        conn.close()
    if fct_count != expected_fct:
        failures.append(f"fct ({fct_count}) != expected ({expected_fct})")
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
    print("\nreconciliation OK: staging is 1:1 with raw; fact matches its source "
          "slice; serving view and CSV report are queryable/existent.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
