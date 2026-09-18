-- Runs once on first container start (docker-entrypoint-initdb.d).
-- Creates the three schemas the pipeline will use across phases:
--   raw      - as-landed data loaded from RustFS, no transformation
--   staging  - cleaned/typed 1:1 models (dbt staging layer)
--   curated  - dimensional/fact models for serving (dbt curated layer)
-- Table DDL is intentionally NOT created here - that is dbt's job from
-- Phase 1 onward. Phase 0 only proves the schemas and connectivity exist.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;
