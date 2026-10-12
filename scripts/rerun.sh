#!/usr/bin/env bash
# Dev convenience: clear (re-run) or trigger the DAG run for SMOKE_DS (default today). Usage: make rerun [SMOKE_DS=2026-10-09]
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

DS="${SMOKE_DS:-$(date -u +%F)}"
EXEC_DATE="${DS}T00:00:00+00:00"

COMPOSE="docker compose -f infra/docker-compose.yml --env-file .env"
AF="$COMPOSE exec -T airflow-scheduler"
EXISTING="$($AF airflow dags state gaming_pipeline "${EXEC_DATE}" 2>/dev/null | tail -1 | tr -d '[:space:]')"
# `dags state` exits 0 and prints "None" when no run exists for the date.
if [ -n "${EXISTING}" ] && [ "${EXISTING}" != "None" ]; then
  $AF airflow tasks clear gaming_pipeline -s "${EXEC_DATE}" -e "${EXEC_DATE}" -y >/dev/null 2>&1
else
  $AF airflow dags trigger gaming_pipeline -e "${EXEC_DATE}" >/dev/null 2>&1
fi
echo "(re)triggered gaming_pipeline for ${DS} - watch it at http://localhost:8080"
