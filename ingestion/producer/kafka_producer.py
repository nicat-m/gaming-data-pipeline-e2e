"""
CLI entrypoint: publishes synthetic game events onto the Kafka topic
configured via KAFKA_TOPIC_GAME_EVENTS.

Usage:
    python -m ingestion.producer.kafka_producer --help
    python -m ingestion.producer.kafka_producer --count 100
    python -m ingestion.producer.kafka_producer --forever
"""
from __future__ import annotations

import argparse
import json
import sys
import time

from config.settings import settings
from ingestion.producer.generator import generate_event


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Produce synthetic gaming events onto a Kafka topic.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of events to emit before exiting (ignored with --forever).",
    )
    parser.add_argument(
        "--forever",
        action="store_true",
        help="Keep producing events until interrupted (Ctrl+C).",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=settings.generator.events_per_second,
        help="Events per second to emit.",
    )
    return parser


def run(count: int, forever: bool, rate: float) -> None:
    # Imported lazily so `--help` works without a reachable Kafka broker.
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    delay = 1.0 / rate if rate > 0 else 0

    emitted = 0
    try:
        while forever or emitted < count:
            event = generate_event(
                dirty_record_rate=settings.generator.dirty_record_rate,
                schema_drift_rate=settings.generator.schema_drift_rate,
            )
            producer.send(settings.kafka.topic_game_events, value=event)
            emitted += 1
            if delay:
                time.sleep(delay)
    finally:
        producer.flush()
        producer.close()
        print(f"Produced {emitted} events to topic '{settings.kafka.topic_game_events}'.")


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    run(count=args.count, forever=args.forever, rate=args.rate)
    return 0


if __name__ == "__main__":
    sys.exit(main())
