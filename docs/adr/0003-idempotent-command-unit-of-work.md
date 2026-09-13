# ADR-0003: Idempotent command unit of work

**Status:** Accepted  **Date:** 2026-08 (reconstructed 2026-09-13)
**Enforced by:** `tests/unit/adapters/test_sqlite_command_uow.py`,
`test_postgres_command_uow.py`; shared core in
`application/command_execution.py`

## Context
Networks retry. A client that times out on a deposit will send it again.
Without idempotency, at-least-once delivery becomes double-crediting.

## Decision
Every command carries a client `command_id`. The unit of work writes the
events and a `command_results` record in ONE transaction; a repeated
`command_id` returns the stored result without re-executing. The decision
logic lives once, in the application layer, and both storage adapters
delegate to it.

## Alternatives rejected
- *Idempotency in the HTTP layer (cache).* Lost on restart; not shared
  across replicas; not transactional with the write.
- *Natural-key dedupe on events.* Cannot represent "same command, rejected
  the first time".

## Consequences
`command_results` grows with every command and needs retention with a
window longer than any client's retry horizon (default 7 days, RUNBOOK R9).
The PostgreSQL autocommit trap (statements committing individually) was a
real bug here and has a regression test.
