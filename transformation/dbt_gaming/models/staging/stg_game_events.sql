-- Phase 0 placeholder: 1:1 passthrough from the raw source, no cleaning or
-- type casting yet. Real staging logic (dedup, type casts, schema-drift
-- handling) is scoped for Phase 1 per docs/PROJECT_PLAN.md.
select *
from {{ source('raw', 'game_events') }}
