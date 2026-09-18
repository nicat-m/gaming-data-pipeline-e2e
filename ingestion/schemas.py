"""
Event/entity shapes produced by the synthetic generator.

These mirror a reduced subset of the "gaming" domain from the Databricks
lakehouse-industry-data-models reference (player, game_title, device,
game_session, in_game_transaction, campaign). Phase 0 only needs these
shapes to exist so the producer/consumer skeletons have something concrete
to import - no transformation logic lives here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class EventType(str, Enum):
    SESSION_START = "session_start"
    SESSION_END = "session_end"
    IN_GAME_TRANSACTION = "in_game_transaction"


@dataclass
class Player:
    player_id: str
    display_name: str
    country: str
    signup_date: str


@dataclass
class Device:
    device_id: str
    platform: str
    os_version: str


@dataclass
class GameTitle:
    game_id: str
    game_name: str
    genre: str


@dataclass
class GameEvent:
    """
    Single raw event emitted onto the `game_events` Kafka topic.

    `extra` carries schema-drift fields (new attributes appearing over
    time) so downstream staging models have a documented place to expect
    the unexpected.
    """

    event_id: str
    event_type: EventType
    event_time: str
    player: Player
    device: Device
    game: GameTitle
    session_id: str | None = None
    amount_usd: float | None = None
    currency: str | None = None
    extra: dict = field(default_factory=dict)
