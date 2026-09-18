"""
Synthetic gaming event generator.

Produces realistic-looking player session / transaction events, with two
deliberate impurities so downstream data-quality and staging logic have
something real to handle in a later phase:

  * dirty_record_rate   - fraction of events with missing/malformed fields
                           (null player_id, negative amount, bad timestamp).
  * schema_drift_rate   - fraction of events that carry an extra, previously
                           undocumented attribute (simulates a mobile SDK
                           upgrade adding a new field mid-stream).

Phase 0 only requires this generator to run and emit well-formed dicts;
no pipeline logic consumes its output yet.
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

from faker import Faker

from ingestion.schemas import Device, EventType, GameEvent, GameTitle, Player

fake = Faker()

_GAMES = [
    GameTitle(game_id="g-001", game_name="Starforge Legends", genre="strategy"),
    GameTitle(game_id="g-002", game_name="Neon Drift Racer", genre="racing"),
    GameTitle(game_id="g-003", game_name="Puzzle Kingdoms", genre="puzzle"),
]

_PLATFORMS = ["android", "ios", "pc", "web"]


def _random_player() -> Player:
    return Player(
        player_id=str(uuid.uuid4()),
        display_name=fake.user_name(),
        country=fake.country_code(),
        signup_date=fake.date_between(start_date="-3y", end_date="today").isoformat(),
    )


def _random_device() -> Device:
    return Device(
        device_id=str(uuid.uuid4()),
        platform=random.choice(_PLATFORMS),
        os_version=f"{random.randint(10, 17)}.{random.randint(0, 9)}",
    )


def _apply_dirtiness(event: GameEvent) -> GameEvent:
    """Corrupt an otherwise-valid event to simulate real-world dirty data."""
    corruption = random.choice(["null_player", "bad_amount", "bad_timestamp"])
    if corruption == "null_player":
        event.player.player_id = None  # type: ignore[assignment]
    elif corruption == "bad_amount" and event.amount_usd is not None:
        event.amount_usd = -abs(event.amount_usd)
    elif corruption == "bad_timestamp":
        event.event_time = "not-a-timestamp"
    return event


def _apply_schema_drift(event: GameEvent) -> GameEvent:
    """Attach an undocumented field, simulating a client-side SDK change."""
    event.extra["ab_test_cohort"] = random.choice(["control", "variant_a", "variant_b"])
    return event


def generate_event(
    dirty_record_rate: float = 0.05,
    schema_drift_rate: float = 0.02,
) -> dict:
    """Build one synthetic game event as a plain dict, ready to serialize."""
    event_type = random.choice(list(EventType))
    event = GameEvent(
        event_id=str(uuid.uuid4()),
        event_type=event_type,
        event_time=datetime.now(timezone.utc).isoformat(),
        player=_random_player(),
        device=_random_device(),
        game=random.choice(_GAMES),
        session_id=str(uuid.uuid4()),
        amount_usd=round(random.uniform(0.99, 49.99), 2)
        if event_type == EventType.IN_GAME_TRANSACTION
        else None,
        currency="USD" if event_type == EventType.IN_GAME_TRANSACTION else None,
    )

    if random.random() < dirty_record_rate:
        event = _apply_dirtiness(event)
    if random.random() < schema_drift_rate:
        event = _apply_schema_drift(event)

    return _event_to_dict(event)


def _event_to_dict(event: GameEvent) -> dict:
    return {
        "event_id": event.event_id,
        "event_type": event.event_type.value,
        "event_time": event.event_time,
        "session_id": event.session_id,
        "amount_usd": event.amount_usd,
        "currency": event.currency,
        "player": vars(event.player),
        "device": vars(event.device),
        "game": vars(event.game),
        **event.extra,
    }
