# Curated layer (fact/dim models)

Intentionally empty in Phase 0 — no business logic or transformations belong
in this phase. Planned models (see `docs/PROJECT_PLAN.md`, section "Data
model"):

- `dim_player` (SCD Type 2)
- `dim_game_title`
- `dim_device`
- `fct_game_session`
- `fct_in_game_transaction`

These will be added in Phase 2 (Transformation) once the staging layer is
built out in Phase 1 (Ingestion & Storage).

`agg_player_daily_engagement` (per-player daily engagement aggregate) is a
planned dbt model for a later phase. See docs/decisions/0004-remove-spark.md
for why there is no separate batch engine.
