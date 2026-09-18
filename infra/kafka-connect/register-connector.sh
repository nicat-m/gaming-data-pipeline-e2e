#!/usr/bin/env bash
# Registers the Debezium PostgreSQL source connector against the Kafka
# Connect REST API once the stack is up. Not run automatically by
# `docker compose up` (Phase 0 keeps ingestion wiring a Phase 1 task) -
# run it manually after the stack is healthy:
#
#   set -a && source .env && set +a
#   ./infra/kafka-connect/register-connector.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"

envsubst < "${SCRIPT_DIR}/postgres-source-connector.json" > /tmp/postgres-source-connector.rendered.json

curl -s -X POST -H "Content-Type: application/json" \
  --data @/tmp/postgres-source-connector.rendered.json \
  "${CONNECT_URL}/connectors" | tee /dev/stderr

rm -f /tmp/postgres-source-connector.rendered.json
