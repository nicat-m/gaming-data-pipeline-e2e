# ADR 0001 — Spark batch job (RustFS S3A → curated aggregate) deferred to Phase 3

Date: 2026-10-09 · Status: **superseded by ADR 0004** (Spark removed) · Phase: 1

## Context

The Phase 0/1 architecture declares Apache Spark as the batch-processing
engine that computes `agg_player_daily_engagement` by reading raw NDJSON
from RustFS via S3A and writing the aggregate back to the curated schema.

Phase 1 requires every architecture component to be *either touched by the
pipeline OR covered by a documented connectivity proof or ADR*.

## Decision

Phase 1 covers the Spark component with a **connectivity proof**: the
Airflow task `spark_connectivity_proof` runs `spark-submit` inside the real
Spark container (`gaming-spark-master`) via the Docker SDK and asserts a
trivial job (`transformation/spark_jobs/connectivity_proof.py`) succeeds.

The real S3A batch job is implemented in Phase 3 (batch processing phase of
the roadmap), because:

1. The S3A classpath (hadoop-aws + aws sdk jars) needs to be added/verified
   for the pinned Spark image — an isolated piece of integration work that
   belongs with the batch-processing phase where it is graded.
2. Phase 1 business logic is explicitly allowed to be naive; the aggregate
   is not on the critical path of the walking skeleton.
3. Avoiding a large custom Spark image keeps the Phase 1 "fresh clone runs
   end to end" requirement fast and robust.

## Consequences

- Phase 1 DoD: Spark is "covered by a documented connectivity proof" — this
  ADR is that documentation, together with the `spark_connectivity_proof`
  task in `orchestration/dags/gaming_pipeline_dag.py`.
- Phase 3 must replace the proof with the real
  `daily_engagement_batch.py` run (S3A read from RustFS, JDBC write to
  `curated.agg_player_daily_engagement`, idempotent per logical date).
- The docker socket mounted read-only into the Airflow containers exists
  only for this proof; Phase 3 will re-evaluate it (DockerOperator or a
  spark-connect based submit) when the real job lands.
