-- Curated fact: one row per in-game transaction event (ClickHouse table).
-- Phase 2 is deliberately naive: dirty amounts (negative USD) are kept as-is
-- and will be filtered/tested properly later. The ReplacingMergeTree table is
-- rebuilt from staging on every run, which is itself idempotent.
{{ config(materialized='table') }}

select
    event_id,
    session_id,
    player_id,
    amount_usd,
    currency,
    event_time,
    source_partition_date
from {{ ref('stg_game_events') }}
where event_type = 'in_game_transaction'
  and event_time is not null
