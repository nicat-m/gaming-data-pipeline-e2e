-- Curated dimension: one row per game, from the external game-catalog API.
-- Current-state (SCD Type 1). Joins to fct_in_game_transaction on game_id.
{{ config(materialized='table') }}

select
    game_id,
    title,
    genre,
    platform,
    publisher,
    developer,
    release_date
from {{ ref('stg_games') }}
