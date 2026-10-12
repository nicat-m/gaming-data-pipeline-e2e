-- Staging: 1:1 typed projection of raw.game_events (ClickHouse).
-- FINAL: ReplacingMergeTree dedupes ASYNC at merge time - reading with FINAL
-- forces the deduped view at query time, so the unique-key DQ test is
-- deterministic even seconds after a raw (re)load.
-- Dirty-record policy: malformed timestamps become NULL (flagged here, counted
-- by the reconciliation report in the smoke test) - rows are never dropped.
{{ config(materialized='view') }}

select
    event_id,
    event_type,
    event_time_raw,
    -- ClickHouse parseDateTimeBestEffort returns NULL-ish 1970 sentinel on
    -- garbage; keep well-formed only, flag the rest as NULL event_time
    if(
        match(event_time_raw, '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}'),
        parseDateTimeBestEffort(event_time_raw),
        NULL
    ) as event_time,
    session_id,
    amount_usd,
    currency,
    player_id,
    player_display_name,
    player_country,
    -- Debezium DATE arrives as epoch-days (int, stored as string in raw):
    -- cast to a real Date here; the raw layer stays dumb (as-landed)
    if(
        match(player_signup_date, '^\\d+$'),
        toDate('1970-01-01') + toInt32(player_signup_date),
        NULL
    ) as player_signup_date,
    device_id,
    device_platform,
    device_os_version,
    game_id,
    game_name,
    game_genre,
    extra,
    toDate(source_partition_date) as source_partition_date,
    loaded_at
from {{ source('raw', 'game_events') }} final
