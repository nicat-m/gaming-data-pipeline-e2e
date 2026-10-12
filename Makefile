COMPOSE := docker compose -f infra/docker-compose.yml --env-file .env
PYTHON ?= .venv/bin/python
COUNT ?= 1000
RATE ?= 5
BATCH_SIZE ?= 200

.DEFAULT_GOAL := help
.PHONY: help portal up down clean ps logs register-connector generate generate-forever consume test

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

portal: ## Open the control panel (all service URLs + copyable credentials)
	@open http://localhost:8099 2>/dev/null || xdg-open http://localhost:8099 2>/dev/null || echo "open http://localhost:8099"

up: ## Start the full infrastructure in the background (incl. first-boot setup)
	$(COMPOSE) up -d
	@echo ">> waiting for the stack to become healthy..."
	@$(COMPOSE) up -d airflow-init superset-init 2>/dev/null || true
	@$(COMPOSE) ps
	@echo ">> control panel (all URLs + copyable credentials): http://localhost:8099"
	@echo ">> next: make smoke  (or the manual steps in README)"
	@echo ">> if this is the FIRST boot: make register-connector && make seed-superset"

down: ## Stop the stack, keep data (named volumes persist)
	$(COMPOSE) down

clean: ## Full reset: stop the stack and delete all volumes (all data lost)
	$(COMPOSE) down -v

ps: ## Show service status and health
	$(COMPOSE) ps

logs: ## Tail logs from all services (Ctrl+C to stop)
	$(COMPOSE) logs -f

register-connector: ## Register the Debezium CDC connector (one-time, stack must be up)
	@set -a && . ./.env && set +a && bash infra/kafka-connect/register-connector.sh

generate: ## Write COUNT synthetic events into source Postgres (make generate COUNT=5000 RATE=1000)
	PYTHONPATH=. $(PYTHON) ingestion/producer/db_writer.py --count $(COUNT) --rate $(RATE)

generate-forever: ## Keep writing events until Ctrl+C
	PYTHONPATH=. $(PYTHON) ingestion/producer/db_writer.py --forever

consume: ## Consume Kafka topic into RustFS batches (runs until Ctrl+C; make consume BATCH_SIZE=50)
	PYTHONPATH=. $(PYTHON) ingestion/consumer/kafka_to_rustfs.py --batch-size $(BATCH_SIZE)

test: ## Run the test suite
	PYTHONPATH=. $(PYTHON) -m pytest -q

smoke: ## End-to-end smoke test (make smoke; or SMOKE_DS=2026-10-08 SMOKE_COUNT=5000 make smoke)
	bash scripts/smoke.sh

rerun: ## Delete today's dag run and trigger a fresh one (dev convenience)
	bash scripts/rerun.sh

seed-superset: ## Create/refresh the Superset dashboard (charts + layout; idempotent)
	docker exec gaming-superset python /app/superset_home/seed.py
