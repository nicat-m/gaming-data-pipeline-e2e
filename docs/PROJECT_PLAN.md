# PROJECT_PLAN — Gaming Player Engagement & Monetization Analytics Platform

## 1. Problem Statement

**Industry:** Gaming (mobile / online multiplayer platform).

**Business question:** Which player segments and which game titles drive
revenue, and where in the session/engagement funnel do we see early churn
signals (drop-off between session start and repeat play, or between first
purchase and repeat purchase)?

**Consumer of the data:** the Product/Growth analytics team.

**Decision the output supports:** which player segments to target with a
retention campaign, and which game titles justify further engineering/
marketing investment. Concretely, the curated `agg_player_daily_engagement`
and `fct_in_game_transaction` models feed a weekly review where Growth
decides campaign budget allocation per game/segment.

**Reference data model:** the entity set (`player`, `game_title`, `device`,
`game_session`, `in_game_transaction`, `campaign`) is a deliberately reduced
subset of the **Gaming** industry model published in
[`databricks-industry-solutions/lakehouse-industry-data-models`](https://github.com/databricks-industry-solutions/lakehouse-industry-data-models/tree/main/data-models/gaming)
(the MVM variant contains 14 domains / 176 tables; this project implements
the "player session & monetization" slice of it end-to-end rather than the
full breadth).

## 2. Data Sources

| Attribute | Detail |
|---|---|
| Origin | Self-written Python synthetic generator (`ingestion/producer/generator.py`) writing directly into a source Postgres OLTP table (`ingestion/producer/db_writer.py`), not a scrape or third-party API, chosen so volume, drift, and dirtiness are fully controllable and reproducible for grading. |
| Format | Relational rows in `source.game_events` (Postgres); Debezium converts each insert into a JSON message on the Kafka topic. |
| Volume estimate | Configurable via `GENERATOR_EVENTS_PER_SECOND` (default 5/s ≈ 432k events/day) — enough to exercise batching/partitioning without needing a cluster. |
| Update frequency | Continuous (streaming) at the source via CDC; landed into the raw zone in micro-batches (Phase 1 default: every 200 messages or on a time-based Airflow schedule). |
| Sample record | See `ingestion/schemas.py::GameEvent` and `infra/postgres-source/init.sql` — flattened columns: `event_id, event_type, event_time_raw, session_id, amount_usd, currency, player_id, player_display_name, player_country, player_signup_date, device_id, device_platform, device_os_version, game_id, game_name, game_genre, extra (jsonb)`. |
| Known quality issues (injected on purpose) | `GENERATOR_DIRTY_RECORD_RATE` fraction of events have a null `player_id`, a negative `amount_usd`, or a malformed `event_time_raw`. `GENERATOR_SCHEMA_DRIFT_RATE` fraction carry an undocumented `ab_test_cohort` key inside the `extra` JSONB column, simulating a client SDK upgrade landing new attributes without a schema migration. |

## 3. Target Architecture

```mermaid
flowchart LR
    subgraph Source
        GEN[Python synthetic\ngenerator]
    end
    subgraph "Ingestion (CDC)"
        PGSRC[(Postgres source\nschema: source)]
        CONNECT[Kafka Connect\nDebezium PostgreSQL connector]
        KAFKA[(Kafka topic\ngaming.source.game_events)]
        CONSUMER[Kafka consumer\n-> batches to object storage]
    end
    subgraph Storage
        RUSTFS[(RustFS\nraw landing zone\nS3-compatible)]
        PGRAW[(Postgres warehouse\nschema: raw)]
    end
    subgraph Transformation
        STG[dbt staging models]
        CUR[dbt curated models\ndims + facts]
        SPARK[Spark batch job\nagg_player_daily_engagement]
    end
    subgraph Orchestration
        AF[Airflow DAG\ngaming_pipeline]
    end
    subgraph Serving
        VIEWS[Postgres SQL views\n/ optional BI tool]
    end

    GEN -->|INSERT| PGSRC
    PGSRC -->|logical replication / WAL| CONNECT
    CONNECT -->|publish| KAFKA
    KAFKA -->|consume| CONSUMER
    CONSUMER -->|put_object, NDJSON batches| RUSTFS
    AF -->|load_raw_to_postgres task| RUSTFS
    AF --> PGRAW
    PGRAW --> STG
    STG --> CUR
    RUSTFS -->|read NDJSON directly| SPARK
    SPARK -->|JDBC write| CUR
    CUR --> VIEWS
    AF -.orchestrates.-> STG
    AF -.orchestrates.-> CUR
    AF -.orchestrates.-> SPARK
```

**Component walkthrough:**

- **Generator** — emits realistic, imperfect events; the only component
  that "invents" data, so every downstream quality problem is traceable to
  a known, documented cause. It writes directly into the source Postgres
  table (`ingestion/producer/db_writer.py`) — it never talks to Kafka.
- **Postgres source (`source.game_events`)** — an OLTP-style table that
  models where a real game backend would actually persist events. Its
  purpose is to give the CDC connector something realistic to watch.
- **Kafka Connect (Debezium PostgreSQL connector)** — reads the source
  database's write-ahead log via logical replication and streams every
  insert onto a Kafka topic, with zero application code publishing to
  Kafka directly. This is the "ingestion" step proper: it decouples the
  OLTP write path from everything downstream and gives exactly-once-ish,
  ordered, replayable delivery.
- **Kafka (`gaming.source.game_events` topic)** — durable, replayable
  buffer between the CDC connector and the raw-storage writer.
- **Kafka consumer → RustFS** — a thin, stateless writer that never
  transforms data; it only batches messages and writes them as
  newline-delimited JSON objects, keyed by offset range for idempotent
  re-runs.
- **RustFS** — S3-compatible object storage acting as the raw landing
  zone / data lake layer, so raw data is durable and queryable by any
  S3-aware tool before it ever touches the warehouse.
- **Postgres warehouse, `raw` schema** — the warehouse-side copy of the
  same raw data, loaded by an Airflow task (`load_raw_to_postgres`) so
  SQL-based transformation (dbt) can operate on it.
- **dbt staging models** — 1:1 cleaned/typed views on top of `raw` (dedup,
  casts, explicit handling of drifted/dirty fields). Materialized as views.
- **dbt curated models** — dimensional/fact tables (`dim_player`,
  `dim_game_title`, `dim_device`, `fct_game_session`,
  `fct_in_game_transaction`) that Growth queries. Materialized as tables.
- **Spark batch job** — reads the raw NDJSON files directly from RustFS
  (S3A) for a given day partition and computes
  `agg_player_daily_engagement`, writing the result into the same
  `curated` schema via JDBC. This is the platform's batch-processing
  engine, applied specifically to the one aggregate that benefits from
  distributed compute rather than row-by-row SQL as raw volume grows — see
  Section 4 for why this job is Spark's and not dbt's.
- **Airflow** — schedules and sequences `load_raw_to_postgres → dbt run
  staging → dbt run curated`, plus `load_raw_to_postgres → spark_batch_
  daily_engagement` in parallel; owns retry/failure handling. Airflow does
  **not** orchestrate the generator, Debezium, or the Kafka consumer —
  those run continuously and independently of the batch DAG.
- **Serving layer** — plain SQL views over the curated schema; a BI tool
  (e.g. Metabase) can point at these directly. Kept deliberately simple —
  no new tool required to serve the data.

## 4. Tech Stack Table

| Component | Chosen technology | Reason | Rejected alternative |
|---|---|---|---|
| Source database | PostgreSQL 16.4 (`postgres-source` service, `wal_level=logical`) | Models a realistic OLTP write path for the generator, and exposes a logical replication slot for CDC | Generator publishing to Kafka directly (skips the realistic "source system" stage entirely) |
| Change data capture | **Kafka Connect + Debezium PostgreSQL connector** (`debezium/connect:2.7.3.Final`) | Standard pattern for "database as the source of truth, stream as the ingestion mechanism"; captures every insert via WAL without touching application code | A custom polling script (reinvents CDC, no schema/offset management, easy to lose events) |
| Streaming backbone | Apache Kafka (`apache/kafka:3.8.0`, KRaft mode) | Durable, replayable buffer between Debezium and the raw-storage writer; standard pairing with Kafka Connect | Direct file drop (no replay, no backpressure handling) |
| Raw object storage (landing zone) | **RustFS** (`rustfs/rustfs:1.0.1`) | S3-compatible, single lightweight Rust binary, trivial to run in Docker Compose, gives a real data-lake landing zone before the warehouse | MinIO (functionally similar; RustFS chosen per project scope to use a different S3-compatible engine); local filesystem (no S3 API, not realistic) |
| Warehouse | PostgreSQL 16.4 (separate instance from the source database) | SQL-based transformation target, widely supported by dbt, easy to run and inspect locally | DuckDB (weaker multi-user/concurrent access story for a pipeline with Airflow + dbt both connecting) |
| Transformation (dims/facts) | dbt-core 1.8.7 + dbt-postgres | Staging → curated layering with built-in tests, docs, and lineage; industry-standard for this exact pattern | Hand-written SQL scripts run by Airflow (no built-in testing/docs, harder to maintain) |
| Batch processing (aggregates) | **Apache Spark** (`bitnami/spark:3.5.3`, standalone master+worker) | Reads raw NDJSON directly from RustFS (S3A) and computes `agg_player_daily_engagement`; gives the platform a genuine distributed batch-compute engine that scales past what row-by-row SQL handles well, and satisfies the requirement to demonstrate batch processing explicitly | Doing the same aggregate in dbt/SQL (works at this data volume, but doesn't demonstrate a distributed batch-processing engine, which is the point of adding this component) |
| Orchestration | Apache Airflow 2.10.2 | DAG-based scheduling, task-level retries, clear dependency graph between load/staging/curated/spark steps | Cron + shell scripts (no dependency graph, no UI, no built-in retry semantics) |
| Data quality | dbt built-in tests (`not_null`, `unique`, `relationships`, `accepted_values`) | Declared alongside the models they test, runs as part of `dbt test`, no extra service | Great Expectations (a second DQ framework — not justified given dbt tests already cover the required checks) |
| Containerization | Docker Compose | Single-command, reproducible local infra for every service above | Manual per-service install (not reproducible, fails the Phase 0 "single command" requirement) |
| Serving | Postgres SQL views on the curated schema | No additional service required; any SQL client or BI tool can consume it | Superset/Metabase as a mandatory component (kept optional) |

> **Note on the "one new tool" rule:** this stack introduces technologies
> that may fall outside the course syllabus — **RustFS**, and depending on
> course coverage, **Kafka Connect/Debezium** and **Spark** as well (Kafka
> itself may already be covered). Each is justified above against a
> concrete requirement (CDC ingestion, data-lake landing zone, batch
> processing) rather than added for its own sake, and each was explicitly
> requested for this iteration of the plan. If the grading rubric strictly
> caps additions at one new tool, the student will confirm with the
> instructor which of RustFS / Kafka Connect / Spark counts as "the one"
> and which are considered already-covered or in-scope extensions — see
> **Risks & Assumptions**.

## 5. Data Model

Source schema → raw → staging → curated, following the reduced
"Gaming MVM" entity subset referenced in Section 1.

| Layer | Storage | Contents | Grain | Keys | Strategy |
|---|---|---|---|---|---|
| Source | Postgres `source.game_events` (OLTP table) | Rows inserted directly by the generator | 1 row = 1 event | `event_id` (primary key) | N/A (append-only table, source of truth) |
| CDC stream | Kafka topic `gaming.source.game_events` | Debezium-published change events, one per insert on `source.game_events` | 1 message = 1 event | `event_id` | N/A (append-only stream, replayable via consumer offsets) |
| Raw | RustFS `raw/game_events/dt=.../*.jsonl`, then Postgres `raw.game_events` | Untouched copy of every consumed event, including dirty/drifted ones | 1 row = 1 event | `event_id` | Append-only; offset range in the object key makes re-loads idempotent (`ON CONFLICT (event_id) DO NOTHING` on load) |
| Staging | Postgres `staging` schema | 1:1 cleaned/typed views per raw entity (`stg_game_events`, later split into `stg_players`, `stg_sessions`, `stg_transactions`) | Same grain as raw | `event_id` | Views, recomputed on every `dbt run`; dirty records are flagged, not silently dropped |
| Curated | Postgres `curated` schema | Dimensional model | See below | See below | Incremental load by `event_time` watermark |

Curated entities (planned, built out in Phase 2 per the roadmap below):

- `dim_player` — **grain:** one row per player per validity period.
  **Key:** `player_id` + `valid_from`. **Strategy: SCD Type 2** — player
  attributes (country, display name) are tracked historically because
  segment analysis needs to know a player's attributes *as of* the session
  being analyzed, not just their current profile.
- `dim_game_title`, `dim_device` — **grain:** one row per natural key.
  **Strategy:** SCD Type 1 (overwrite) — attributes here are stable enough
  that history isn't analytically useful.
- `fct_game_session` — **grain:** one row per session. **Key:**
  `session_id`. **Strategy:** incremental append, watermark on
  `event_time`.
- `fct_in_game_transaction` — **grain:** one row per purchase. **Key:**
  `event_id`/`transaction_id`. **Strategy:** incremental append, watermark
  on `event_time`.
- `agg_player_daily_engagement` — **grain:** one row per `player_id` per
  calendar day. **Strategy:** full-refresh per day partition (rebuilt from
  `fct_game_session` + `fct_in_game_transaction` for that day).

## 6. Pipeline Design

- **Orchestration approach:** a single Airflow DAG (`gaming_pipeline`,
  `orchestration/dags/gaming_pipeline_dag.py`) covering the batch portion
  only: `load_raw_to_postgres → dbt_run_staging → dbt_run_curated`, and
  `load_raw_to_postgres → spark_batch_daily_engagement` in parallel. The
  generator, Debezium connector, and Kafka consumer are **not** Airflow
  tasks — they run continuously as long-lived processes/services,
  independent of the DAG schedule.
- **Scheduling:** the CDC path (generator → source Postgres → Debezium →
  Kafka → consumer → RustFS) is continuous/streaming by nature. The
  Airflow DAG that picks up from RustFS is batch, not triggered per-event.
  Phase 0/1 default is manual trigger; Phase 3 will set an hourly
  `schedule` once the DAG has real task logic, matching the
  "near-real-time but not sub-minute" latency the business question needs.
- **Batch vs. streaming:** hybrid, now with three distinct stages: (1)
  streaming CDC ingestion (Debezium/Kafka), (2) batch load + SQL
  transformation (Airflow + dbt), (3) batch distributed aggregation
  (Spark, reading raw files directly from RustFS rather than through the
  warehouse). This mirrors a common real-world pattern where the source is
  a stream but analytics only need periodic freshness, and one specific
  aggregate is heavy enough to warrant a dedicated compute engine.
- **Idempotency / re-run strategy:** RustFS object keys embed the Kafka
  offset range consumed into that file, so re-running the consumer for the
  same offsets overwrites the same object rather than duplicating it.
  Postgres raw/source loads use `event_id` as a natural dedup key
  (`ON CONFLICT DO NOTHING`). dbt models are pure `SELECT`s recomputed from
  `raw`/`staging`, so re-running `dbt run` is naturally idempotent. The
  Spark job is scoped to reprocess one `--date` partition at a time, so
  re-running it for the same date is a full overwrite of that partition,
  not an append.
- **Failure handling:** Airflow task-level `retries: 2` (see
  `default_args` in the DAG). A failed `load_raw_to_postgres` blocks both
  the dbt tasks and the Spark task (both depend on it), so bad raw data
  never silently reaches curated. Dirty records are not dropped at load
  time — they are carried through to staging and flagged, so failures are
  visible in dbt test results rather than hidden by pre-filtering. If
  Debezium's replication slot falls behind or the connector fails, it is
  visible via the Kafka Connect REST API (`GET /connectors/<name>/status`)
  independent of the Airflow DAG's own health.
- **Expected runtime:** at the default generator rate (5 events/s), one
  hourly Airflow run processes ≈ 18,000 events; the full
  load → staging/curated/spark DAG run is expected to complete in well
  under a minute in local Docker Compose (validated once Phase 1/2/3 task
  logic exists).

## 7. Data Quality Plan

| Check | Where enforced | On failure |
|---|---|---|
| `event_id` not null, unique | dbt test on `staging.stg_game_events` | `dbt test` fails, curated build is not triggered (Airflow task fails before `dbt_run_curated`) |
| `player_id` not null | dbt test on `dim_player` | Row flagged/excluded from `dim_player`, counted in a `dbt test` failure so it's visible, not silently dropped upstream |
| `amount_usd >= 0` for transactions | dbt test (`accepted_range` / custom singular test) on `fct_in_game_transaction` | Row excluded from the fact table pending manual review; count of excluded rows logged |
| `event_time` parses as a valid timestamp | staging cast with `try_cast`-style handling + not-null test on the cast column | Unparseable rows kept in `raw` (audit trail) but excluded from staging/curated |
| Foreign key integrity (`session_id`, `player_id`, `game_id` all resolve) | dbt `relationships` test between fact and dimension tables | `dbt test` failure surfaces in Airflow logs; pipeline does not silently publish orphaned facts |
| Schema drift (`extra`/`ab_test_cohort` field) | Staging model explicitly extracts known drift fields into a documented column instead of dropping them | New/unexpected keys are preserved in a raw JSON column for later inspection, never silently discarded |

Data quality checks are dbt tests (Section 4 justifies this choice over a
separate DQ framework). They run as part of `dbt test`, which Phase 2 will
add as an explicit Airflow task between staging and curated.

## 8. Phase Roadmap

| Phase | Scope | Definition of Done |
|---|---|---|
| **Phase 0 — Infrastructure & skeleton** (this delivery) | Repo, directory structure, Docker Compose infra (source Postgres, warehouse Postgres, Kafka, Kafka Connect/Debezium, RustFS, Spark, Airflow), config, placeholder ingestion/orchestration/transformation modules, docs | All Phase 0 DoD items in the assignment (see README "Current Status") |
| **Phase 1 — Ingestion & Storage** | Register the Debezium connector for real; generator/consumer run continuously; raw data lands in RustFS and Postgres `raw` schema; implement `load_raw_to_postgres` | A scheduled Airflow run moves events end-to-end from `source.game_events` through CDC into `raw.game_events` with zero data loss and idempotent re-runs |
| **Phase 2 — Transformation** | Real staging models (cleaning, casts, drift handling) and curated dimensional model (Section 5) built in dbt | `dbt run` + `dbt test` succeed on a populated warehouse; curated tables match the documented grain/keys |
| **Phase 3 — Batch Processing & Orchestration** | Implement the Spark `agg_player_daily_engagement` job for real (S3A read from RustFS, JDBC write to curated); DAG scheduled hourly; `dbt test` wired in as a blocking task; alerting on failure | A full scheduled run completes unattended, including the Spark task; a deliberately corrupted batch is caught by a dbt test and blocks curated refresh |
| **Phase 4 — Serving & Consumption** | SQL views for the Growth team's actual questions; optional BI dashboard | A named business question from Section 1 can be answered with one query against the serving layer |
| **Phase 5 — Branching workflow & hardening** | PR-based Git workflow, CI running dbt tests, documentation pass | PRs required for `main`, CI green on a sample PR |

## 9. Risks & Assumptions

| Risk / assumption | Mitigation |
|---|---|
| RustFS, Kafka Connect/Debezium, and/or Spark may exceed the "one new tool" allowance if not covered by the course | Flagged explicitly in Section 4; will confirm with instructor before Phase 1 which additions are acceptable and fall back (e.g. a JDBC polling script instead of Debezium, or a dbt/SQL aggregate instead of Spark) if disallowed |
| RustFS is a newer project with a smaller community than MinIO; fewer references for troubleshooting | Pinned to a specific tag (`1.0.1`); S3 API compatibility means the consumer/Spark code only depends on `boto3`/S3A, so swapping back to MinIO would be a config-only change if RustFS proves unstable |
| Debezium requires `wal_level=logical` and a dedicated replication slot; misconfiguration silently stalls CDC | `postgres-source` sets `wal_level=logical` explicitly in Compose; Phase 1 will add a health check on the connector's `/status` endpoint, not just container health |
| Running Spark (master+worker) alongside Airflow, 2x Postgres, Kafka, Kafka Connect, and RustFS is a heavy footprint for a laptop | Spark worker capped to 1 core / 1 GB; all images pinned and lightweight where possible; documented as a known resource trade-off in the README prerequisites |
| Synthetic data may not convincingly resemble real telemetry volume/shape | Generator parameters (`GENERATOR_*` env vars) are tunable; Faker-based realistic player/device attributes; dirty/drift rates documented and adjustable |
| Idempotency claims (Section 6) are only validated once Phase 1/3 ship real task logic | Explicitly scoped as Phase 1/3 Definition of Done items, not claimed as done in Phase 0 |

## 10. How to Run

Prerequisites: Docker + Docker Compose v2, Python 3.11+.

```bash
# 1. Clone and configure
git clone <repo-url> && cd gaming-data-pipeline-e2e
cp .env.example .env
# edit .env: set real values for every <CHANGE_ME> (see comments in the file
# for how to generate each one)

# 2. Start infrastructure
docker compose -f infra/docker-compose.yml up -d

# 3. Check every service is healthy
docker compose -f infra/docker-compose.yml ps

# 4. Airflow UI: http://localhost:8080 (login: AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD from .env)
# RustFS console: http://localhost:9001
# Spark master UI: http://localhost:8081
# Kafka Connect REST API: http://localhost:8083

# 5. Register the Debezium PostgreSQL source connector (one-time, after the stack is healthy)
set -a && source .env && set +a
./infra/kafka-connect/register-connector.sh

# 6. Local Python environment (for running the generator/tests outside Docker)
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 7. Run tests
PYTHONPATH=. pytest -q

# 8. Try the generator/consumer CLIs (require the stack from step 2 running)
PYTHONPATH=. python -m ingestion.producer.db_writer --count 50
PYTHONPATH=. python -m ingestion.consumer.kafka_to_rustfs --batch-size 50 --max-batches 1

# 9. Stop infrastructure (state persists in named Docker volumes)
docker compose -f infra/docker-compose.yml down

# 10. Full reset (drops all volumes/state)
docker compose -f infra/docker-compose.yml down -v
```
