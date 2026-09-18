-- Source-side OLTP table that the synthetic generator writes into directly.
-- Debezium's PostgreSQL connector (Kafka Connect) uses logical replication
-- (wal_level=logical, see infra/docker-compose.yml) to stream every INSERT
-- on this table into the `game_events` Kafka topic - no application code
-- publishes to Kafka directly.
--
-- Columns are a flattened version of ingestion/schemas.py::GameEvent:
--   - player_id is nullable on purpose (simulates dirty records).
--   - extra is a catch-all JSONB column for schema-drift attributes
--     (e.g. ab_test_cohort) so new fields never require a table migration.

CREATE SCHEMA IF NOT EXISTS source;

CREATE TABLE IF NOT EXISTS source.game_events (
    event_id        UUID PRIMARY KEY,
    event_type      TEXT NOT NULL,
    event_time_raw  TEXT NOT NULL, -- kept as TEXT: dirty records may contain a malformed timestamp
    session_id      UUID,
    amount_usd      NUMERIC,
    currency        TEXT,
    player_id       UUID,
    player_display_name TEXT,
    player_country  TEXT,
    player_signup_date DATE,
    device_id       UUID NOT NULL,
    device_platform TEXT NOT NULL,
    device_os_version TEXT,
    game_id         TEXT NOT NULL,
    game_name       TEXT,
    game_genre      TEXT,
    extra           JSONB NOT NULL DEFAULT '{}'::jsonb,
    inserted_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
