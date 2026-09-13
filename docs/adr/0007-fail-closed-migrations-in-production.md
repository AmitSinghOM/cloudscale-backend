# ADR-0007: Fail-closed migrations in production

**Status:** Accepted  **Date:** 2026-09-11
**Enforced by:** `tests/unit/adapters/test_postgres_migrations.py`;
`/v1/ready` re-verifies the revision

## Context
An application that creates its own tables will happily run against a
database it did not migrate, silently diverging from the schema the code
expects.

## Decision
`CLOUDSCALE_PG_SCHEMA=auto` (dev/test) creates tables idempotently.
`CLOUDSCALE_PG_SCHEMA=migrations` (production) creates NOTHING and refuses
to start unless Alembic has stamped the database at `CURRENT_REVISION`.
The DDL has one source (`adapters/postgres/schema.py`) used by both paths,
and a test asserts the two resulting schemas are identical.

## Alternatives rejected
- *Auto-migrate on startup.* Concurrent replicas racing DDL; no rollback
  story; hides schema drift.
- *Migrations only, no auto mode.* Slows local development and tests for
  no safety gain in those contexts.

## Consequences
Every schema change is an Alembic revision plus a `CURRENT_REVISION` bump
(e.g. `0002` for retention). Deploys must run `migrate upgrade` first
(RUNBOOK R1).
