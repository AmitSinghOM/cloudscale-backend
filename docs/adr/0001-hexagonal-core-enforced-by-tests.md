# ADR-0001: Hexagonal core enforced by tests

**Status:** Accepted  **Date:** 2026-08 (reconstructed 2026-09-13)
**Enforced by:** `tests/architecture/test_dependency_boundaries.py`

## Context
Frameworks (FastAPI, psycopg, Kafka clients, OpenTelemetry) have shorter
lives than the money-handling rules they serve. If domain logic imports
them, every framework migration becomes a rewrite of the rules.

## Decision
`cloudscale.domain` and `cloudscale.application` import only the standard
library and each other. Adapters (`cloudscale.adapters`) and entrypoints
(`cloudscale.entrypoints`) may import frameworks. A test walks the import
graph and fails the build on violation.

## Alternatives rejected
- *Convention only.* Drifts within months; the first hot fix imports the
  ORM "just this once".
- *Separate packages/repos per layer.* Correct but heavy for one service;
  the import test buys the same guarantee at zero release cost.

## Consequences
Swapping SQLite for PostgreSQL required no domain change (proved: ADR-0004).
Any future transport or store is an adapter. Cost: ports (`application/ports.py`)
must be designed rather than leaked.
