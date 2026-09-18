"""
Central configuration for the Gaming Lakehouse pipeline.

Every setting is read from environment variables (see .env.example for the
full list and safe sample values). No component should read os.environ
directly outside this module - import `settings` instead so there is a
single source of truth for configuration.
"""
from __future__ import annotations

import os

from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()


class PostgresSettings(BaseModel):
    user: str = os.getenv("POSTGRES_USER", "gaming_app")
    password: str = os.getenv("POSTGRES_PASSWORD", "")
    db: str = os.getenv("POSTGRES_DB", "gaming_lakehouse")
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.db}"
        )


class KafkaSettings(BaseModel):
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_game_events: str = os.getenv("KAFKA_TOPIC_GAME_EVENTS", "game_events")


class RustFSSettings(BaseModel):
    endpoint_url: str = os.getenv("RUSTFS_ENDPOINT_URL", "http://localhost:9000")
    access_key: str = os.getenv("RUSTFS_ACCESS_KEY", "")
    secret_key: str = os.getenv("RUSTFS_SECRET_KEY", "")
    raw_bucket: str = os.getenv("RUSTFS_RAW_BUCKET", "raw-game-events")


class GeneratorSettings(BaseModel):
    events_per_second: float = float(os.getenv("GENERATOR_EVENTS_PER_SECOND", "5"))
    dirty_record_rate: float = float(os.getenv("GENERATOR_DIRTY_RECORD_RATE", "0.05"))
    schema_drift_rate: float = float(os.getenv("GENERATOR_SCHEMA_DRIFT_RATE", "0.02"))


class Settings(BaseModel):
    postgres: PostgresSettings = PostgresSettings()
    kafka: KafkaSettings = KafkaSettings()
    rustfs: RustFSSettings = RustFSSettings()
    generator: GeneratorSettings = GeneratorSettings()


settings = Settings()
