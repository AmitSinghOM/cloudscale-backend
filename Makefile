PYTHON ?= python3.12
VENV ?= .venv
VENV_PYTHON := $(VENV)/bin/python
VENV_RUFF := $(VENV)/bin/ruff
VENV_MYPY := $(VENV)/bin/mypy

.DEFAULT_GOAL := check

.PHONY: venv install install-dev format format-check lint type test coverage check resolved

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(VENV_PYTHON) -m pip install --require-hashes -r requirements.lock

install-dev: venv
	$(VENV_PYTHON) -m pip install --require-hashes -r requirements-dev.lock

format:
	$(VENV_RUFF) format .

format-check:
	$(VENV_RUFF) format --check .

lint:
	$(VENV_RUFF) check .

type:
	$(VENV_MYPY) cloudscale cqrs

test:
	$(VENV_PYTHON) -m pytest -q

coverage:
	$(VENV_PYTHON) -m pytest --cov=cloudscale --cov=cqrs --cov-report=term-missing --cov-fail-under=85

check: format-check lint type test

resolved:
	$(VENV_PYTHON) -m pip list --format=freeze | LC_ALL=C sort

# -- local developer loop (SQLite tier, no infrastructure) -------------------
# `make dev` starts the API and the projection consumer in the background with
# a fixed DEVELOPMENT secret; `make token` mints a bearer token for it;
# `make stop` tears both down. Nothing here is for production (RUNBOOK D4).
DEV_DIR      ?= .dev
DEV_PORT     ?= 8000
DEV_SECRET   ?= local-dev-only-secret-0123456789abcdef-0123456789abcdef
DEV_ENV       = CLOUDSCALE_STORAGE=sqlite \
                CLOUDSCALE_LOG_DB=$(DEV_DIR)/log.db \
                CLOUDSCALE_PROJECTION_DB=$(DEV_DIR)/projection.db \
                CLOUDSCALE_JWT_SECRET=$(DEV_SECRET)

dev: venv
	@if lsof -nP -iTCP:$(DEV_PORT) -sTCP:LISTEN >/dev/null 2>&1; then \
	    echo "port $(DEV_PORT) is already in use by another process."; \
	    echo "  either stop it, or: make dev DEV_PORT=8123   (then: make token DEV_PORT=8123)"; \
	    exit 1; fi
	@mkdir -p $(DEV_DIR)
	@$(DEV_ENV) $(VENV_PYTHON) -m uvicorn --factory cloudscale.entrypoints.http.main:build_app \
	    --host 127.0.0.1 --port $(DEV_PORT) --log-level warning \
	    > $(DEV_DIR)/server.log 2>&1 & echo $$! > $(DEV_DIR)/server.pid
	@$(DEV_ENV) $(VENV_PYTHON) -m cloudscale.entrypoints.consumer_loop \
	    > $(DEV_DIR)/consumer.log 2>&1 & echo $$! > $(DEV_DIR)/consumer.pid
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
	    curl -sf http://127.0.0.1:$(DEV_PORT)/v1/ready >/dev/null 2>&1 && break; sleep 0.5; done
	@echo "API      http://127.0.0.1:$(DEV_PORT)   (docs: /docs, ready: /v1/ready, metrics: /metrics)"
	@echo "logs     $(DEV_DIR)/server.log  $(DEV_DIR)/consumer.log"
	@echo "next     make token        # or: make token ARGS=--curl"
	@echo "stop     make stop"

token: venv
	@CLOUDSCALE_JWT_SECRET=$(DEV_SECRET) $(VENV_PYTHON) scripts/dev_token.py --port $(DEV_PORT) $(ARGS)

stop:
	@for p in server consumer; do \
	    if [ -f $(DEV_DIR)/$$p.pid ]; then kill $$(cat $(DEV_DIR)/$$p.pid) 2>/dev/null || true; rm -f $(DEV_DIR)/$$p.pid; fi; done
	@echo "stopped (data kept in $(DEV_DIR)/; delete the directory to reset)"

.PHONY: dev token stop
