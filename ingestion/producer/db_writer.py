"""
CLI entrypoint: writes synthetic game events as INSERTs into the source
Postgres OLTP table (`source.game_events`), instead of publishing to Kafka
directly. Kafka Connect's Debezium PostgreSQL connector (see
infra/kafka-connect/) picks up these inserts via logical replication (CDC)
and streams them onto the `game_events` Kafka topic - the generator never
talks to Kafka itself.

Pipeline: this script -> Postgres (source) -> Debezium/Kafka Connect ->
Kafka -> ingestion/consumer/kafka_to_rustfs.py -> RustFS.

Usage:
    python -m ingestion.producer.db_writer --help
    python -m ingestion.producer.db_writer --count 100
    python -m ingestion.producer.db_writer --forever
"""
from __future__ import annotations

import argparse
import sys
import time

from config.settings import settings
from ingestion.producer.generator import generate_event

INSERT_SQL = """
INSERT INTO source.game_events (
    event_id, event_type, event_time_raw, session_id, amount_usd, currency,
    player_id, player_display_name, player_country, player_signup_date,
    device_id, device_platform, device_os_version,
    game_id, game_name, game_genre, extra
) VALUES (
    %(event_id)s, %(event_type)s, %(event_time_raw)s, %(session_id)s, %(amount_usd)s, %(currency)s,
    %(player_id)s, %(player_display_name)s, %(player_country)s, %(player_signup_date)s,
    %(device_id)s, %(device_platform)s, %(device_os_version)s,
    %(game_id)s, %(game_name)s, %(game_genre)s, %(extra)s
)
ON CONFLICT (event_id) DO NOTHING
"""


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Write synthetic gaming events into the source Postgres table.",
    )
    parser.add_argument("--count", type=int, default=100, help="Number of events to write (ignored with --forever).")
    parser.add_argument("--forever", action="store_true", help="Keep writing events until interrupted (Ctrl+C).")
    parser.add_argument(
        "--rate",
        type=float,
        default=settings.generator.events_per_second,
        help="Events per second to write.",
    )
    return parser


def _to_row(event: dict) -> dict:
    """Flatten a nested generator event dict into source.game_events columns."""
    player = event["player"]
    device = event["device"]
    game = event["game"]
    extra = {k: v for k, v in event.items() if k not in {
        "event_id", "event_type", "event_time", "session_id", "amount_usd",
        "currency", "player", "device", "game",
    }}
    return {
        "event_id": event["event_id"],
        "event_type": event["event_type"],
        "event_time_raw": event["event_time"],
        "session_id": event["session_id"],
        "amount_usd": event["amount_usd"],
        "currency": event["currency"],
        "player_id": player["player_id"],
        "player_display_name": player["display_name"],
        "player_country": player["country"],
        "player_signup_date": player["signup_date"],
        "device_id": device["device_id"],
        "device_platform": device["platform"],
        "device_os_version": device["os_version"],
        "game_id": game["game_id"],
        "game_name": game["game_name"],
        "game_genre": game["genre"],
        "extra": _json_dumps(extra),
    }


def _json_dumps(value: dict) -> str:
    import json

    return json.dumps(value)


def run(count: int, forever: bool, rate: float) -> None:
    # Imported lazily so `--help` works without a reachable Postgres instance.
    import psycopg2

    conn = psycopg2.connect(
        host=settings.source_postgres.host,
        port=settings.source_postgres.port,
        user=settings.source_postgres.user,
        password=settings.source_postgres.password,
        dbname=settings.source_postgres.db,
    )
    conn.autocommit = True
    delay = 1.0 / rate if rate > 0 else 0

    written = 0
    try:
        with conn.cursor() as cur:
            while forever or written < count:
                event = generate_event(
                    dirty_record_rate=settings.generator.dirty_record_rate,
                    schema_drift_rate=settings.generator.schema_drift_rate,
                )
                cur.execute(INSERT_SQL, _to_row(event))
                written += 1
                if delay:
                    time.sleep(delay)
    finally:
        conn.close()
        print(f"Wrote {written} events to source.game_events.")


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    run(count=args.count, forever=args.forever, rate=args.rate)
    return 0


if __name__ == "__main__":
    sys.exit(main())
