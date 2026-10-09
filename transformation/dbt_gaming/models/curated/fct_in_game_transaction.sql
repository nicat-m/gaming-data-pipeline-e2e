-- Curated fact: one row per in-game transaction event.
-- Phase 1 is deliberately naive: dirty amounts (negative USD) are kept as-is
-- and will be filtered/tested properly in Phase 2. TODO Phase 2.
-- The table is materialized as a full-refresh table rebuilt from staging on
-- every run, which is itself idempotent (no duplicate rows on re-run).
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
