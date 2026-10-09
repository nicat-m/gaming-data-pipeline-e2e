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
    parser.add_argument(
        "--logical-date",
        default=None,
        help=(
            "Partition date (YYYY-MM-DD) used in the raw object keys. "
            "Defaults to today (UTC). The Airflow DAG passes {{ ds }} here so "
            "a run for logical date D lands objects under dt=D and a re-run "
            "overwrites the same keys instead of duplicating data."
        ),
    )
    parser.add_argument(
        "--idle-exit-seconds",
        type=float,
        default=0,
        help=(
            "Exit once no new Kafka message arrives for this many seconds "
            "(0 = wait forever). Set to a small value when the consumer is "
            "orchestrated (Airflow) so a run with no new events still ends."
        ),
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


def _write_batch(
    s3_client,
    bucket: str,
    records: list[dict],
    first_offset: int,
    last_offset: int,
    partition_date: str,
) -> str:
    key = f"raw/game_events/dt={partition_date}/{first_offset}-{last_offset}.jsonl"
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    return key


def run(
    batch_size: int,
    max_batches: int | None,
    partition_date: str | None = None,
    idle_exit_seconds: float = 0,
) -> dict:
    """Consume available messages and land them in RustFS.

    Returns a small summary dict (messages read, batches written) so the
    orchestrating Airflow task can log rows read/written per Phase 1
    logging requirements.
    """
    # Imported lazily so `--help` works without a reachable Kafka broker.
    from kafka import KafkaConsumer

    partition_date = partition_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    # `consumer_timeout_ms` makes `for message in consumer` raise StopIteration
    # after this idle window - that is how a bounded, orchestrated run ends
    # when the broker has no (more) new events.
    consumer_timeout_ms = int(idle_exit_seconds * 1000) if idle_exit_seconds > 0 else None

    consumer = KafkaConsumer(
        settings.kafka.topic_game_events,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="rustfs-landing-writer",
        consumer_timeout_ms=consumer_timeout_ms,
    )
    s3_client = _get_s3_client()
    _ensure_bucket(s3_client, settings.rustfs.raw_bucket)

    buffer: list[dict] = []
    first_offset = None
    batches_written = 0
    messages_read = 0
    started = datetime.now(timezone.utc)

    print(
        f"[consumer] start: topic={settings.kafka.topic_game_events} "
        f"partition_date={partition_date} batch_size={batch_size} "
        f"max_batches={max_batches or 'unbounded'} "
        f"idle_exit_seconds={idle_exit_seconds or 'off'}"
    )
    try:
        for message in consumer:
            if first_offset is None:
                first_offset = message.offset
            buffer.append(message.value)
            messages_read += 1

            if len(buffer) >= batch_size:
                key = _write_batch(
                    s3_client, settings.rustfs.raw_bucket, buffer,
                    first_offset, message.offset, partition_date,
                )
                print(f"[consumer] wrote batch to s3://{settings.rustfs.raw_bucket}/{key} ({len(buffer)} messages)")
                buffer = []
                first_offset = None
                batches_written += 1
                if max_batches is not None and batches_written >= max_batches:
                    break
    except StopIteration:
        print(f"[consumer] idle for {idle_exit_seconds}s - no new messages, exiting.")

    if buffer:
        # Flush the tail batch (fewer than batch_size messages).
        key = _write_batch(
            s3_client, settings.rustfs.raw_bucket, buffer,
            first_offset, first_offset + len(buffer) - 1, partition_date,
        )
        print(f"[consumer] wrote tail batch to s3://{settings.rustfs.raw_bucket}/{key} ({len(buffer)} messages)")
        batches_written += 1

    consumer.close()
    summary = {
        "messages_read": messages_read,
        "batches_written": batches_written,
        "partition_date": partition_date,
        "elapsed_seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 2),
    }
    print(f"[consumer] end: {summary}")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    run(
        batch_size=args.batch_size,
        max_batches=args.max_batches,
        partition_date=args.logical_date,
        idle_exit_seconds=args.idle_exit_seconds,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
