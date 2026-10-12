# ADR 0002 — ClickHouse replaces PostgreSQL as the warehouse; Superset added as the consumer

Date: 2026-10-12 · Status: accepted · Phase: 1 (change), 2 (Superset)

## Context

The Phase 0 plan used a second PostgreSQL instance as the warehouse
(`raw` / `staging` / `curated` schemas) with dbt-postgres, and left the
consumer as "any SQL client". During Phase 1/2 two things became clear:

- the workload is append-heavy event data with daily aggregations, which is
  what a columnar engine is built for;
- the consumer needs a reproducible, visible serving output (dashboard), not
  just a SQL view.

## Decision

- The warehouse is **ClickHouse 24.8** (`raw`, `staging`, `curated`,
  `serving` databases created by `infra/clickhouse/init.sql`), transformed by
  **dbt-clickhouse**.
- Raw idempotency uses a `ReplacingMergeTree` ordered by
  `(source_partition_date, event_id)`; staging reads with `FINAL` so
  pre-merge duplicates never reach curated/serving.
- **Apache Superset 4.1.1** is the consumer. Its database connection, datasets,
  charts and dashboard are created from the repository
  (`infra/superset/`, `scripts/superset_seed.py`, `make seed-superset`).
- The old warehouse Postgres service was removed; `POSTGRES_*` variables
  remain in `.env.example` only as legacy connection settings.

## Consequences

- `ON CONFLICT` upserts no longer exist; deduplication is merge-based
  (eventually) plus `FINAL` at read time. The smoke test reconciles with
  `uniq(event_id)` to account for pre-merge duplicates.
- One more heavy container (Superset); RAM requirement rises to ~8 GB.
- Rejected: keeping Postgres (poor fit for analytical scans at scale);
  Metabase/Grafana (less suited to SQL-defined, provisioned dashboards).
