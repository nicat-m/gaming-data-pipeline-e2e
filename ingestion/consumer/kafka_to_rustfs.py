"""
CLI entrypoint: consumes raw events from Kafka and lands them, unmodified,
as newline-delimited JSON batch files in RustFS (S3-compatible object
storage) under the raw landing zone bucket.

Object key layout: raw/game_events/dt=<YYYY-MM-DD>/<first_offset>-<last_offset>.jsonl
The offset range in the key makes re-runs idempotent: re-processing the same
offsets overwrites the same object instead of duplicating data.

Usage:
    python -m ingestion.consumer.kafka_to_rustfs --help
    python -m ingestion.consumer.kafka_to_rustfs --batch-size 500 --max-batches 1
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from config.settings import settings


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Consume Kafka game events and land them as batch files in RustFS.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Number of messages to buffer before writing one object to RustFS.",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Stop after writing this many batches (default: run forever).",
    )
    return parser


def _get_s3_client():
    # Imported lazily so `--help` works without reachable RustFS/boto3 creds.
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=settings.rustfs.endpoint_url,
        aws_access_key_id=settings.rustfs.access_key,
        aws_secret_access_key=settings.rustfs.secret_key,
    )


def _ensure_bucket(s3_client, bucket: str) -> None:
    existing = {b["Name"] for b in s3_client.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        s3_client.create_bucket(Bucket=bucket)


def _write_batch(s3_client, bucket: str, records: list[dict], first_offset: int, last_offset: int) -> str:
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"raw/game_events/dt={dt}/{first_offset}-{last_offset}.jsonl"
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    return key


def run(batch_size: int, max_batches: int | None) -> None:
    # Imported lazily so `--help` works without a reachable Kafka broker.
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        settings.kafka.topic_game_events,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="rustfs-landing-writer",
    )
    s3_client = _get_s3_client()
    _ensure_bucket(s3_client, settings.rustfs.raw_bucket)

    buffer: list[dict] = []
    first_offset = None
    batches_written = 0

    for message in consumer:
        if first_offset is None:
            first_offset = message.offset
        buffer.append(message.value)

        if len(buffer) >= batch_size:
            key = _write_batch(s3_client, settings.rustfs.raw_bucket, buffer, first_offset, message.offset)
            print(f"Wrote batch to s3://{settings.rustfs.raw_bucket}/{key}")
            buffer = []
            first_offset = None
            batches_written += 1
            if max_batches is not None and batches_written >= max_batches:
                break

    consumer.close()


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    run(batch_size=args.batch_size, max_batches=args.max_batches)
    return 0


if __name__ == "__main__":
    sys.exit(main())
