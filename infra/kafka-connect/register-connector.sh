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
CONNECTOR_NAME="gaming-postgres-source-connector"

envsubst < "${SCRIPT_DIR}/postgres-source-connector.json" > /tmp/postgres-source-connector.rendered.json

# Idempotent: PUT (update) if the connector already exists, POST (create) if
# not - safe to run repeatedly (the smoke test relies on this).
if curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}" > /dev/null 2>&1; then
  echo "connector '${CONNECTOR_NAME}' exists - updating config"
  # PUT /config takes ONLY the config map, not the {"name","config"} wrapper.
  python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["config"]))' \
    /tmp/postgres-source-connector.rendered.json > /tmp/postgres-source-connector.config.json
  curl -s -X PUT -H "Content-Type: application/json" \
    --data @/tmp/postgres-source-connector.config.json \
    "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" > /dev/null
  echo "connector config updated"
else
  echo "registering new connector '${CONNECTOR_NAME}'"
  curl -s -X POST -H "Content-Type: application/json" \
    --data @/tmp/postgres-source-connector.rendered.json \
    "${CONNECT_URL}/connectors"
fi
echo

rm -f /tmp/postgres-source-connector.rendered.json /tmp/postgres-source-connector.config.json
