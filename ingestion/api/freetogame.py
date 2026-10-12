"""Ingestion of the external game-catalog API (FreeToGame) into the raw zone.

Second source of the pipeline (the first is generator -> Postgres -> CDC ->
Kafka). The API is real and public (no key); this module is the "ingestion
component" for it: HTTP fetch with timeout + retry/backoff, minimal
validation, and a write to RustFS under

    raw/games_catalog/dt=<logical date>/games.jsonl

The object key is deterministic per logical date, so a re-run overwrites the
same object (idempotent). Rows are stored as received (one JSON per line).
"""
from __future__ import annotations

import json
import time

import requests

from config.settings import settings

REQUIRED_FIELDS = ("id", "title")
OBJECT_KEY = "raw/games_catalog/dt={ds}/games.jsonl"


class GamesApiError(RuntimeError):
    """The API was unreachable or returned an unusable payload."""


def fetch_games(
    url: str | None = None,
    timeout: float | None = None,
    attempts: int = 3,
    backoff_seconds: float = 2.0,
    http_get=requests.get,
) -> list[dict]:
    """GET the catalog. Retries transient failures (network, 5xx, 429)."""
    url = url or settings.games_api.url
    timeout = timeout or settings.games_api.timeout_seconds
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            resp = http_get(url, timeout=timeout, headers={"Accept": "application/json"})
            if resp.status_code == 429 or resp.status_code >= 500:
                raise GamesApiError(f"HTTP {resp.status_code} from {url}")
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, list):
                raise GamesApiError(f"expected a JSON list, got {type(payload).__name__}")
            return payload
        except (requests.RequestException, ValueError, GamesApiError) as exc:
            last_error = exc
            print(f"[freetogame] attempt {attempt}/{attempts} failed: {exc}")
            if attempt < attempts:
                time.sleep(backoff_seconds * attempt)
    raise GamesApiError(f"games API unavailable after {attempts} attempts: {last_error}")


def validate(games: list[dict]) -> tuple[list[dict], int]:
    """Keep records that carry the required fields; return (valid, rejected_count)."""
    valid = [g for g in games if isinstance(g, dict) and all(g.get(f) not in (None, "") for f in REQUIRED_FIELDS)]
    return valid, len(games) - len(valid)


def land_games(partition_date: str, s3_client=None) -> dict:
    """Fetch the catalog and land it in the RustFS raw zone for `partition_date`."""
    from ingestion.consumer.kafka_to_rustfs import _ensure_bucket, _get_s3_client

    games = fetch_games()
    valid, rejected = validate(games)
    if not valid:
        raise GamesApiError("games API returned no valid records")
    s3 = s3_client or _get_s3_client()
    bucket = settings.rustfs.raw_bucket
    _ensure_bucket(s3, bucket)
    key = OBJECT_KEY.format(ds=partition_date)
    body = "\n".join(json.dumps(g, ensure_ascii=False) for g in valid).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body)
    return {"rows_read": len(games), "rows_written": len(valid), "rows_rejected": rejected,
            "bucket": bucket, "key": key}
