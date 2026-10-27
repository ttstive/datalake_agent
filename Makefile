COMPOSE := docker compose
KAFKA_BIN := /opt/kafka/bin
INTERNAL_BOOTSTRAP := kafka:29092
TOPIC ?= orders
MAX ?= 10
ARGS ?=

# Databricks CLI: not on PATH; default to the one shipped with the VS Code extension. Override: make deploy DATABRICKS=databricks
DATABRICKS ?= $(HOME)/.vscode/extensions/databricks.databricks-2.22.0-darwin-arm64/bin/databricks
# Auth profile from ~/.databrickscfg. Override: make deploy DATABRICKS_PROFILE=other
DATABRICKS_PROFILE ?= svc_residencia
export DATABRICKS_CONFIG_PROFILE := $(DATABRICKS_PROFILE)
CATALOG ?= workspace
SCHEMA ?= ecommerce
VOLUME ?= landing

.PHONY: help kafka-up kafka-down kafka-ui kafka-logs topics consume produce test lint \
	db-whoami db-setup ingest-build ingest-up ingest-down ingest-logs validate deploy run-pipeline run-bronze \
	schedule-on schedule-off schedule-status \
	mlflow-up mlflow-ui mlflow-logs agent-metrics-backfill agent-evals

help:
	@echo "Targets: kafka-up kafka-down kafka-ui kafka-logs topics consume produce test lint"
	@echo "         db-whoami db-setup ingest-build ingest-up ingest-down ingest-logs validate deploy"
	@echo "         run-pipeline (alias: run-bronze) schedule-on schedule-off schedule-status"
	@echo "         mlflow-up mlflow-ui mlflow-logs agent-metrics-backfill agent-evals"

kafka-up: ## Start Kafka + UI (http://127.0.0.1:8080), wait until healthy, create the orders topic
	$(COMPOSE) up -d --wait kafka kafka-ui
	$(COMPOSE) run --rm kafka-init

kafka-ui: ## Open the Kafka web UI
	open http://127.0.0.1:8080

kafka-down: ## Stop containers (data volume kept; add -v manually to wipe)
	$(COMPOSE) down

kafka-logs:
	$(COMPOSE) logs -f --tail=100 kafka

topics: ## List topics and describe orders
	$(COMPOSE) exec kafka $(KAFKA_BIN)/kafka-topics.sh --bootstrap-server $(INTERNAL_BOOTSTRAP) --list
	$(COMPOSE) exec kafka $(KAFKA_BIN)/kafka-topics.sh --bootstrap-server $(INTERNAL_BOOTSTRAP) --describe --topic $(TOPIC)

consume: ## Read up to MAX messages (default 10) from the start of TOPIC; stops after 10s idle
	$(COMPOSE) exec kafka $(KAFKA_BIN)/kafka-console-consumer.sh \
		--bootstrap-server $(INTERNAL_BOOTSTRAP) --topic $(TOPIC) \
		--from-beginning --max-messages $(MAX) --timeout-ms 10000 \
		--property print.key=true --property print.partition=true \
		2>&1 | grep -v -e 'TimeoutException' -e 'ERROR Error processing message' || true

produce: ## Run the fake-order producer, e.g. make produce ARGS="--rate 5 --duration 30"
	uv run python -m datalake_duckdb.producer $(ARGS)

test:
	uv run pytest

lint:
	uv run ruff check .

# ---- Databricks -------------------------------------------------------------

db-whoami: ## Check CLI auth (prints user name only)
	@$(DATABRICKS) current-user me -o json | python3 -c 'import sys,json; print(json.load(sys.stdin)["userName"])'

db-setup: ## Create UC schema + managed landing volume if absent (idempotent)
	@$(DATABRICKS) schemas get $(CATALOG).$(SCHEMA) >/dev/null 2>&1 \
		|| $(DATABRICKS) schemas create $(SCHEMA) $(CATALOG) --comment "Streaming ELT: orders (landing/bronze/silver/gold)" >/dev/null
	@$(DATABRICKS) volumes read $(CATALOG).$(SCHEMA).$(VOLUME) >/dev/null 2>&1 \
		|| $(DATABRICKS) volumes create $(CATALOG) $(SCHEMA) $(VOLUME) MANAGED --comment "Raw Parquet micro-batches from local spark-ingestor" >/dev/null
	@echo "ok: $(CATALOG).$(SCHEMA) and /Volumes/$(CATALOG)/$(SCHEMA)/$(VOLUME)"

validate: ## Validate the Asset Bundle (dev target)
	$(DATABRICKS) bundle validate -t dev

deploy: validate ## Validate + deploy the bundle to dev (job schedule stays PAUSED)
	$(DATABRICKS) bundle deploy -t dev

run-pipeline: ## Run the orders_ingestion job once (bronze -> silver -> gold) and wait
	$(DATABRICKS) bundle run -t dev orders_ingestion

run-bronze: run-pipeline ## Deprecated alias: the job now runs bronze -> silver -> gold

# Schedule toggle goes through the bundle (var schedule_pause_status), so the deployed
# state always matches what `bundle deploy` would set. NOTE: a plain `make deploy` resets it to PAUSED.
schedule-on: validate ## UNPAUSE the 5-min schedule. COSTS MONEY: serverless compute every 5 min (288 runs/day)
	@echo "WARNING: orders_ingestion will run on serverless compute every 5 minutes until 'make schedule-off'."
	$(DATABRICKS) bundle deploy -t dev --var="schedule_pause_status=UNPAUSED"

schedule-off: ## Pause the 5-min schedule (redeploys the bundle with PAUSED)
	$(DATABRICKS) bundle deploy -t dev --var="schedule_pause_status=PAUSED"

schedule-status: ## Show the deployed job's schedule pause_status
	@$(DATABRICKS) bundle summary -t dev -o json | python3 -c 'import sys,json; j=json.load(sys.stdin)["resources"]["jobs"]["orders_ingestion"]; print(j.get("schedule",{}).get("pause_status"))'

# ---- Local Spark ingestor (Kafka -> UC Volume) ------------------------------
# Needs DATABRICKS_TOKEN in the root .env (compose interpolates it; never baked into the image).

ingest-build: ## Build the spark-ingestor image
	$(COMPOSE) --profile ingest build spark-ingestor

ingest-up: ## Start the Spark ingestor (Spark UI: http://127.0.0.1:4040)
	$(COMPOSE) --profile ingest up -d --build spark-ingestor

ingest-down: ## Stop the Spark ingestor (checkpoint volume kept)
	$(COMPOSE) --profile ingest stop spark-ingestor
	$(COMPOSE) --profile ingest rm -f spark-ingestor

ingest-logs:
	$(COMPOSE) --profile ingest logs -f --tail=100 spark-ingestor

# ---- MLflow (agent metrics now, MLOps later) --------------------------------

mlflow-up: ## Start the MLflow server only (http://127.0.0.1:5000) and wait until healthy
	$(COMPOSE) up -d --wait mlflow

mlflow-ui: ## Open the MLflow web UI
	open http://127.0.0.1:5000

mlflow-logs:
	$(COMPOSE) logs -f --tail=100 mlflow

agent-metrics-backfill: ## Log token/context metrics of all existing Claude Code transcripts of this project
	uv run python observability/claude_mlflow_hook.py --backfill

agent-evals: ## Score the Claude Code team against the project rules (MLflow experiment claude-agents-evals), e.g. make agent-evals ARGS="--session ID --no-mlflow"
	uv run python observability/agent_evals.py $(ARGS)
