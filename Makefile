PYTHON ?= python3.12
VENV ?= .venv
VENV_PYTHON := $(VENV)/bin/python
VENV_RUFF := $(VENV)/bin/ruff
VENV_MYPY := $(VENV)/bin/mypy

.DEFAULT_GOAL := check

.PHONY: help venv install install-dev format format-check lint type test coverage check resolved fresh-tree

help:  ## list targets
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | sed -E 's/^([a-zA-Z_-]+):[^#]*## /  \1|/' | sort | column -t -s '|'

venv:  ## create the virtualenv
	$(PYTHON) -m venv $(VENV)

install: venv  ## hash-verified runtime install (requirements.lock)
	$(VENV_PYTHON) -m pip install --require-hashes -r requirements.lock

install-dev: venv  ## hash-verified dev install (requirements-dev.lock)
	$(VENV_PYTHON) -m pip install --require-hashes -r requirements-dev.lock

format:  ## ruff format
	$(VENV_RUFF) format .

format-check:  ## ruff format --check
	$(VENV_RUFF) format --check .

lint:  ## ruff check (incl. bandit S rules)
	$(VENV_RUFF) check .

type:  ## mypy
	$(VENV_MYPY) cloudscale cqrs

test:  ## pytest (PostgreSQL-gated tests need CLOUDSCALE_TEST_PG)
	$(VENV_PYTHON) -m pytest -q

coverage:  ## pytest with coverage
	$(VENV_PYTHON) -m pytest --cov=cloudscale --cov=cqrs --cov-report=term-missing --cov-fail-under=85

check: format-check lint type test  ## full gate (default)

fresh-tree:  ## prove HEAD works from exactly what git tracks (install + gate in a temp export)
	scripts/check_fresh_tree.sh

resolved:  ## print the resolved dependency set
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

dev: venv  ## start API + consumer on the SQLite tier (DEV_PORT=8000)
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
	@curl -sf http://127.0.0.1:$(DEV_PORT)/v1/ready >/dev/null 2>&1 || { \
	    echo "server did not become ready on :$(DEV_PORT); last lines of $(DEV_DIR)/server.log:"; \
	    tail -20 $(DEV_DIR)/server.log; $(MAKE) -s stop; exit 1; }
	@kill -0 $$(cat $(DEV_DIR)/consumer.pid) 2>/dev/null || { \
	    echo "consumer exited at startup; last lines of $(DEV_DIR)/consumer.log:"; \
	    tail -20 $(DEV_DIR)/consumer.log; $(MAKE) -s stop; exit 1; }
	@echo "API      http://127.0.0.1:$(DEV_PORT)   (docs: /docs, ready: /v1/ready, metrics: /metrics)"
	@echo "logs     $(DEV_DIR)/server.log  $(DEV_DIR)/consumer.log"
	@echo "next     make token        # or: make token ARGS=--curl"
	@echo "stop     make stop"

token: venv  ## mint a dev bearer token; ARGS=--curl prints a paste-ready deposit
	@CLOUDSCALE_JWT_SECRET=$(DEV_SECRET) $(VENV_PYTHON) scripts/dev_token.py --port $(DEV_PORT) $(ARGS)

stop:  ## stop the dev stack; data kept in .dev/
	@for p in server consumer; do \
	    if [ -f $(DEV_DIR)/$$p.pid ]; then kill $$(cat $(DEV_DIR)/$$p.pid) 2>/dev/null || true; rm -f $(DEV_DIR)/$$p.pid; fi; done
	@echo "stopped (data kept in $(DEV_DIR)/; delete the directory to reset)"

.PHONY: dev token stop
