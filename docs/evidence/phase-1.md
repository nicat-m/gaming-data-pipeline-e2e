# Phase 1 Evidence

Reviewer-facing proof for Phase 1 DoD. Screenshots are captured by the
student during the live run (they cannot be produced from CI).

## 1. Successful orchestrator run

> **[SCREENSHOT 1 — Airflow UI, Graph view of `gaming_pipeline`, all tasks
> green (success) for a manual run triggered for a specific logical date.]**

Reproduce: `make smoke` (uses `SMOKE_DS=${date -u +%F}`), or trigger
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

Replace the numbers below with the output of the last smoke run:

```text
layer                     | rows
--------------------------+-----------
raw.game_events (ds=...)  | <FILL>
raw.game_events (total)   | <FILL>
staging.stg_game_events   | <FILL>
  flagged dirty (NULL ts) | <FILL>
curated.fct_transactions  | <FILL>
curated.dim_player        | <FILL>
serving view rows         | <FILL>
serving CSV report        | exists=True
```

Reconciliation rule (asserted by `scripts/smoke_verify.py`):

- `staging == raw total` — staging is a 1:1 typed view over raw.
- `fct == staging rows where event_type='in_game_transaction' and event_time IS NOT NULL`
  — the difference vs. staging is explained: session rows and rows whose
  malformed timestamps were flagged to NULL (dirty-record policy).
- Re-running the same logical date adds no rows (idempotency:
  offset-range object keys + `ON CONFLICT (event_id) DO NOTHING` +
  full-refresh table models).

## 4. Restart persistence

> **[SCREENSHOT 3 (optional) — `docker compose down` then `up -d`, then the
> row-count table again: unchanged, and the pipeline re-runs successfully.]**

```bash
make down && make up
make smoke
```
