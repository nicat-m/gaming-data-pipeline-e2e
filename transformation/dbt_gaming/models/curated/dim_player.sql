-- Curated dimension: one row per player.
-- Phase 1 is deliberately naive (latest-attribute-wins, SCD Type 1 style);
-- historical attribute tracking (SCD Type 2) is Phase 2 scope - see
-- docs/PROJECT_PLAN.md section 5. TODO Phase 2.
{{ config(materialized='table') }}

select
    player_id,
    max(player_display_name) as display_name,
    max(player_country)      as country,
    min(player_signup_date)  as signup_date
from {{ ref('stg_game_events') }}
where player_id is not null
group by player_id
