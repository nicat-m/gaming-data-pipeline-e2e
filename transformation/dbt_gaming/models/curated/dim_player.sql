-- Curated dimension: one row per player.
-- Phase 2 is deliberately naive (latest-attribute-wins, SCD Type 1 style);
-- historical attribute tracking (SCD Type 2) is a later scope. TODO.
{{ config(materialized='table') }}

select
    player_id,
    argMax(player_display_name, loaded_at) as display_name,
    argMax(player_country, loaded_at)      as country,
    min(player_signup_date)                as signup_date
from {{ source('raw', 'game_events') }}
where player_id is not null
group by player_id
