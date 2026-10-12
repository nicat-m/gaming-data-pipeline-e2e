-- Staging: typed projection of the LATEST game-catalog snapshot (FreeToGame API).
-- raw.games_catalog keeps one snapshot per logical date; staging exposes only
-- the most recent one so that game_id is unique (DQ test) and dim_game is a
-- plain current-state dimension (SCD1; history is a later-phase TODO).
{{ config(materialized='view') }}

select
    game_id,
    title,
    genre,
    platform,
    publisher,
    developer,
    toDateOrNull(release_date)       as release_date,
    short_description,
    game_url,
    toDate(source_partition_date)    as snapshot_date
from {{ source('raw', 'games_catalog') }} final
where source_partition_date = (select max(source_partition_date) from {{ source('raw', 'games_catalog') }})
