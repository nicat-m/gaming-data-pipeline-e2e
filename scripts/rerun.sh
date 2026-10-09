#!/usr/bin/env bash
# Dev convenience: drop the DAG run for SMOKE_DS (default today) and trigger
# a fresh manual run. Usage: make rerun [SMOKE_DS=2026-10-09]
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

DS="${SMOKE_DS:-$(date -u +%F)}"
EXEC_DATE="${DS}T00:00:00+00:00"

docker exec gaming-airflow-postgres psql -U airflow -d airflow \
  -c "DELETE FROM task_instance WHERE dag_id='gaming_pipeline'" \
  -c "DELETE FROM dag_run WHERE dag_id='gaming_pipeline'" > /dev/null
echo "dag run for ${DS} removed"
docker exec gaming-airflow-scheduler airflow dags trigger gaming_pipeline -e "${EXEC_DATE}" > /dev/null 2>&1
echo "triggered gaming_pipeline for ${DS} - watch it at http://localhost:8080"
