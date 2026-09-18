COMPOSE := docker compose -f infra/docker-compose.yml --env-file .env
PYTHON ?= .venv/bin/python
COUNT ?= 1000

.DEFAULT_GOAL := help
.PHONY: help up down clean ps logs register-connector generate generate-forever consume test

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

up: ## Start the full infrastructure in the background
	$(COMPOSE) up -d
	$(COMPOSE) ps

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

generate: ## Write COUNT synthetic events into source Postgres (make generate COUNT=5000)
	PYTHONPATH=. $(PYTHON) ingestion/producer/db_writer.py --count $(COUNT)

generate-forever: ## Keep writing events until Ctrl+C
	PYTHONPATH=. $(PYTHON) ingestion/producer/db_writer.py --forever

consume: ## Consume Kafka topic into RustFS batches (runs until Ctrl+C)
	PYTHONPATH=. $(PYTHON) ingestion/consumer/kafka_to_rustfs.py

test: ## Run the test suite
	PYTHONPATH=. $(PYTHON) -m pytest -q
