-- Runs once on first container start (docker-entrypoint-initdb.d).
-- Creates the warehouse schemas (ClickHouse "databases") the pipeline uses:
--   raw      - as-landed data loaded from RustFS, no transformation
--   staging  - cleaned/typed 1:1 models (dbt staging layer)
--   curated  - dimensional/fact models for serving (dbt curated layer)
--   serving  - dashboard-facing views / flattened aggregates
-- Table DDL is created by dbt from Phase 2 onward (dbt-clickhouse adapter).

CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS staging;
CREATE DATABASE IF NOT EXISTS curated;
CREATE DATABASE IF NOT EXISTS serving;
