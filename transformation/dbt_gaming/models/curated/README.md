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

`agg_player_daily_engagement` is **not** built by dbt - it is owned by the
Spark batch job at `transformation/spark_jobs/daily_engagement_batch.py`,
which reads raw NDJSON directly from RustFS and writes the aggregate into
this same `curated` schema via JDBC. See docs/PROJECT_PLAN.md section 3
for the reasoning behind this split.
