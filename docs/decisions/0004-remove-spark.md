# ADR 0004 — Remove Apache Spark from the architecture

Date: 2026-10-13 · Status: accepted · Supersedes: ADR 0001

## Context

ADR 0001 kept Spark in the architecture and covered it in Phase 1 with a
connectivity proof (an Airflow task running `spark.range(0, 1000).count()`
in the Spark container); the real S3A aggregation was deferred to Phase 3.

In practice Spark did no useful work: no layer, view or dashboard depended
on it. At this data volume (a few thousand to tens of thousands of rows per
day) every planned aggregate fits comfortably in ClickHouse/dbt SQL. Keeping
it cost resources (2 containers, ~1 GB RAM), a docker-socket mount into
Airflow (only used by the proof) and extra surface to explain and defend.

## Decision

Remove Spark entirely: compose services (`spark-master`, `spark-worker`),
the `spark_connectivity_proof` DAG task, `SPARK_*` configuration,
`pyspark`, the Airflow docker-socket mount, `transformation/spark_jobs/`,
and the documentation references.

Batch aggregates (e.g. `agg_player_daily_engagement`) will be implemented as
dbt models in the curated layer when needed.

## Consequences

- Fewer moving parts and lower resource use; the Airflow container no longer
  needs the host docker socket.
- The architecture no longer demonstrates a distributed batch engine. If a
  later requirement needs one (volume growth, a job that does not fit SQL),
  Spark can be reintroduced with a real S3A job and a new ADR.
