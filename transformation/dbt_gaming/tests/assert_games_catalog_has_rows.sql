-- DQ check (singular test): the API-sourced game catalog must not be empty,
-- otherwise dim_game would be empty and every game-level metric meaningless.
select count(*) as catalog_rows
from {{ ref('stg_games') }}
having count(*) = 0
