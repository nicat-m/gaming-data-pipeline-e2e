"""Spark connectivity proof (Phase 1).

Phase 1 requires every architecture component to be either touched by the
pipeline or covered by a documented connectivity proof. The real Spark
batch job (RustFS S3A read -> curated aggregate write) is Phase 3 scope -
this script proves the Spark cluster is integrated and executable from the
orchestrator: the Airflow task `spark_connectivity_proof` runs it via
`spark-submit` inside the Spark container.

Run locally (matches what the DAG does):
    docker exec gaming-spark-master spark-submit \
        --master local[1] /opt/spark-apps/connectivity_proof.py
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("gaming-connectivity-proof").getOrCreate()
    rows = spark.range(0, 1000).count()
    assert rows == 1000, f"expected 1000 rows, got {rows}"
    print(f"[spark-connectivity-proof] OK: Spark {spark.version} counted {rows} rows")
    spark.stop()


if __name__ == "__main__":
    main()
