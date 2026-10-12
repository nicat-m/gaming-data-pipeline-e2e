-- DQ check 4 (singular test): staging must contain at least one row for the
-- data that has landed. A run against an empty raw layer fails here, which
-- blocks curated/serving downstream instead of publishing empty outputs.
select count(*) as staging_rows
from {{ ref('stg_game_events') }}
having count(*) = 0
