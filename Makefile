COMPOSE ?= docker compose
DAG_ID := cnpj_medallion
# One-off containers go through the image entrypoint, which registers the host UID in
# /etc/passwd (the JVM/Hadoop need a username); the DB wait is skipped since postgres
# is not started for these commands.
RUN := $(COMPOSE) run --rm --no-deps -e CONNECTION_CHECK_MAX_COUNT=0 airflow-scheduler
PY_SOURCES := src dags tests
WEB_PORT = $(or $(shell grep -E '^AIRFLOW_WEB_PORT=' .env 2>/dev/null | cut -d= -f2),8080)

.DEFAULT_GOAL := help
.PHONY: help up down restart logs ps register-dag run run-release test lint format \
        venv test-local lint-local clean

help: ## List available commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.env:
	cp .env.example .env
	echo "AIRFLOW_UID=$$(id -u)" >> .env

up: .env ## Build images and start Airflow (UI on http://localhost:8080, admin/admin)
	$(COMPOSE) up -d --build
	@echo "Airflow UI: http://localhost:$(WEB_PORT)"

down: ## Stop containers (keeps data and metadata)
	$(COMPOSE) down

restart: down up ## Restart the stack

logs: ## Tail scheduler logs
	$(COMPOSE) logs -f airflow-scheduler

ps: ## Show container status
	$(COMPOSE) ps

register-dag:
	$(COMPOSE) exec airflow-scheduler airflow dags reserialize
	$(COMPOSE) exec airflow-scheduler airflow dags unpause $(DAG_ID)

run: register-dag ## Unpause and trigger the DAG for the latest release
	$(COMPOSE) exec airflow-scheduler airflow dags trigger $(DAG_ID)

run-release: register-dag ## Trigger a release: make run-release RELEASE=2026-10-13 [FORCE=true]
	@test -n "$(RELEASE)" || (echo "usage: make run-release RELEASE=YYYY-MM-DD" && exit 1)
	$(COMPOSE) exec airflow-scheduler airflow dags trigger $(DAG_ID) \
		--conf '{"release": "$(RELEASE)", "force": $(or $(FORCE),false)}'

test: .env ## Run the test suite inside the Airflow image
	$(RUN) python -m pytest

lint: .env ## flake8 + black --check inside the Airflow image
	$(RUN) bash -c "flake8 $(PY_SOURCES) && black --check $(PY_SOURCES)"

format: .env ## Apply black formatting
	$(RUN) python -m black $(PY_SOURCES)

venv: ## Create a local .venv (needs Python 3.9+ and Java 11/17 for Spark)
	python3 -m venv .venv
	.venv/bin/pip install -r requirements-dev.txt

test-local: ## Run tests in the local .venv
	.venv/bin/python -m pytest

lint-local: ## Lint in the local .venv
	.venv/bin/flake8 $(PY_SOURCES)
	.venv/bin/black --check $(PY_SOURCES)

clean: ## Stop everything and delete volumes, lake data and logs
	$(COMPOSE) down -v
	rm -rf data logs
