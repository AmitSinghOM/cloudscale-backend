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
