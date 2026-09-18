"""
Phase 0 skeleton for the Spark batch job that will compute
`curated.agg_player_daily_engagement`.

Ownership split with dbt (see docs/PROJECT_PLAN.md, section 3 & 5):
  - dbt owns the dimensional model (dim_player, dim_game_title, dim_device,
    fct_game_session, fct_in_game_transaction) via SQL against Postgres.
  - Spark owns this one aggregate: it reads the raw NDJSON batch files
    directly from RustFS (S3A, one day-partition at a time) and writes the
    daily engagement aggregate into Postgres `curated` via JDBC. This gives
    the platform a genuine batch-processing engine for the one job that
    would not scale through row-by-row SQL as raw volume grows.

Phase 0 only proves the environment (Spark master/worker + this script are
reachable via spark-submit); no aggregation logic is implemented yet - see
docs/PROJECT_PLAN.md phase roadmap, Phase 2/3.

Usage (submitted from the `spark-submit` client, e.g. via Airflow
BashOperator/SparkSubmitOperator in a later phase):

    spark-submit \
        --master ${SPARK_MASTER_URL} \
        transformation/spark_jobs/daily_engagement_batch.py --help
"""
from __future__ import annotations

import argparse
import sys


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Batch-compute curated.agg_player_daily_engagement from RustFS raw events.",
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Day partition to process, format YYYY-MM-DD (matches raw/game_events/dt=<date>/ in RustFS).",
    )
    return parser


def run(date: str) -> None:
    """
    Phase 2 will implement: create a SparkSession configured with the
    S3A filesystem pointed at RustFS, read
    s3a://<raw_bucket>/raw/game_events/dt=<date>/*.jsonl, aggregate session
    and transaction counts per player, and write the result to
    curated.agg_player_daily_engagement via the Postgres JDBC driver.
    """
    raise NotImplementedError("Implemented in Phase 2 - Transformation.")


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    run(date=args.date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
