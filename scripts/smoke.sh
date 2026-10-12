#!/usr/bin/env bash
# Phase 1 integration smoke test - one command, real services only.
#
#   make smoke                     # today's logical date, 5000 seeded rows
#   SMOKE_DS=2026-10-08 make smoke # a specific logical date
#
# Steps: (1) check the stack is up, (2) seed the source with COUNT synthetic
# events, (3) make sure the Debezium connector is registered, (4) trigger the
# Airflow DAG for the logical date and poll until it finishes, (5) verify
# every layer (raw, staging, curated, serving) and reconcile row counts.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

DS="${SMOKE_DS:-$(date -u +%F)}"
COUNT="${SMOKE_COUNT:-5000}"
RATE="${SMOKE_RATE:-1000}"   # events/sec: high enough that seeding takes seconds
EXEC_DATE="${DS}T00:00:00+00:00"
COMPOSE="docker compose -f infra/docker-compose.yml --env-file .env"

echo "== smoke: logical date ${DS}, seeding ${COUNT} events =="

echo "== 1/5 stack health =="
$COMPOSE ps

echo "== 2/5 seed source (generator -> source Postgres) =="
make generate COUNT="${COUNT}" RATE="${RATE}"

echo "== 3/5 ensure Debezium CDC connector (idempotent) =="
set -a && . ./.env && set +a
bash infra/kafka-connect/register-connector.sh

echo "== 4/5 trigger DAG run for ${DS} and poll (timeout 10 min) =="
AF="$COMPOSE exec -T airflow-scheduler"
$AF airflow dags unpause gaming_pipeline
# Idempotent: if a run for this logical date already exists, clear it (a
# normal Airflow re-run); otherwise trigger a new one. Other dates' runs and
# history are left untouched.
EXISTING="$($AF airflow dags state gaming_pipeline "${EXEC_DATE}" 2>/dev/null | tail -1 | tr -d '[:space:]')"
# `dags state` exits 0 and prints "None" when no run exists for the date.
if [ -n "${EXISTING}" ] && [ "${EXISTING}" != "None" ]; then
  $AF airflow tasks clear gaming_pipeline -s "${EXEC_DATE}" -e "${EXEC_DATE}" -y >/dev/null 2>&1
  echo "   existing run for ${DS} cleared and re-queued"
else
  $AF airflow dags trigger gaming_pipeline -e "${EXEC_DATE}" >/dev/null
  echo "   triggered for ${DS}"
fi

STATE="queued"
for _ in $(seq 1 60); do
  sleep 10
  STATE="$($AF airflow dags state gaming_pipeline "${EXEC_DATE}" 2>/dev/null | tail -1 | awk '{print $NF}')"
  echo "   run state: ${STATE:-<none>}"
  case "${STATE:-}" in
    success) break ;;
    failed) echo "== smoke FAILED: orchestrator run marked the run failed ==" ; exit 1 ;;
  esac
done
if [ "${STATE:-}" != "success" ]; then
  echo "== smoke FAILED: run did not reach success in time (last state: ${STATE}) =="
  exit 1
fi
echo "== orchestrator run finished with success =="

echo "== 5/5 layer verification and reconciliation =="
PYTHONPATH=. .venv/bin/python scripts/smoke_verify.py "${DS}"

echo "== smoke PASSED =="
