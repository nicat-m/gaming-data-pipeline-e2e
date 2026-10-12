# Phase 1 Evidence

Reviewer-facing proof for Phase 1 DoD. Screenshots are captured by the
student during the live run (they cannot be produced from CI).

## 1. Successful orchestrator run

> **[SCREENSHOT 1 — Airflow UI, Graph view of `gaming_pipeline`, all tasks
> green (success) for a manual run triggered for a specific logical date.]**

Reproduce: `make smoke` (logical date defaults to today, UTC), or trigger
manually for a specific date:

```bash
docker exec gaming-airflow-scheduler airflow dags trigger \
  gaming_pipeline -e 2026-10-09T00:00:00+00:00
```

## 2. Deliberate failure — downstream tasks stop

> **[SCREENSHOT 2 — Airflow UI, same DAG, run failed: task
> `land_raw_kafka_to_rustfs` red with `AirflowFailException`, all
> downstream tasks marked upstream_failed/none.]**

Reproduce:

```bash
PIPELINE_FAIL_SWITCH=1 make up   # recreates airflow containers with the switch on
make smoke                       # -> run fails at the ingestion task
# back to normal:
PIPELINE_FAIL_SWITCH=0 make up
```

## 3. Row counts per layer (from the smoke test)

Output of `SMOKE_DS=2026-10-11 SMOKE_COUNT=1000 make smoke` — the **third**
run for that logical date, after a run for 2026-10-12 had also been loaded:

```text
layer                            | rows
---------------------------------+-----------
raw.game_events (ds=2026-10-11)  | 7000
  distinct event_id (ds)         | 7000
raw.game_events (total)          | 12000
  distinct event_id (total)      | 12000
staging.stg_game_events(ds)      | 7000
staging.stg_game_events          | 12000
  flagged dirty (NULL ts)        | 193
curated.fct_transactions         | 3860
curated.dim_player               | 11799
serving view rows                | 3801
serving CSV report               | serving/player_engagement_daily/dt=2026-10-11/report.csv exists=True
raw rows per logical date        | 2026-10-11: 7000 (7000)   2026-10-12: 5000 (5000)
```

(Counts grow between runs only because each `make smoke` first seeds new
synthetic events into the source; the point is that rows == distinct events
for every date, i.e. no duplicates after repeated runs, and that both logical
dates coexist.)

Reconciliation rules (asserted by `scripts/smoke_verify.py`):

- `raw rows == raw distinct event_id` per date — the raw load is
  delete-then-insert for the logical date plus in-run dedup on `event_id`.
- `staging == raw distinct` — staging is a 1:1 typed view over raw
  (`FINAL` on the ReplacingMergeTree as a safety net).
- `fct == staging rows where event_type='in_game_transaction' and event_time IS NOT NULL`
  — the difference vs. staging is explained: session rows and rows whose
  malformed timestamps were flagged to NULL (dirty-record policy).
- Raw ≥ 1000 rows for the logical date.

Re-running the same logical date is safe: raw objects are keyed by Kafka
offset range, the raw load clears and reloads only that date's partition,
curated models are full-refresh and serving is `CREATE OR REPLACE VIEW`.
`make smoke` re-runs an existing date with `airflow tasks clear` instead of
deleting run history.

## 4. Restart persistence

> **[SCREENSHOT 3 (optional) — `docker compose down` then `up -d`, then the
> row-count table again: unchanged, and the pipeline re-runs successfully.]**

```bash
make down && make up
make smoke
```
