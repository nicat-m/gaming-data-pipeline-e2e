-- Staging: 1:1 typed projection of raw.game_events (Phase 1 walking-skeleton
-- logic, deliberately naive - deeper cleaning is Phase 2 scope).
-- Dirty-record policy: malformed timestamps become NULL (flagged here, counted
-- by the reconciliation report in the smoke test) - rows are never dropped.
{{ config(materialized='view') }}

select
    event_id,
    event_type,
    event_time_raw,
    -- naive cast: only well-formed ISO timestamps survive, others -> NULL
    case
        when event_time_raw ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
        then event_time_raw::timestamptz
    end as event_time,
    session_id,
    amount_usd,
    currency,
    player_id,
    player_display_name,
    player_country,
    player_signup_date,
    device_id,
    device_platform,
    device_os_version,
    game_id,
    game_name,
    game_genre,
    extra,
    source_partition_date,
    loaded_at
from {{ source('raw', 'game_events') }}
