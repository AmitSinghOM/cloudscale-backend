# ADR-0005: Transactional outbox on PostgreSQL

**Status:** Accepted  **Date:** 2026-09-10
**Enforced by:** outbox skew regression test in
`tests/unit/adapters/test_postgres_tier.py`

## Context
Under concurrency, a transaction that obtained a lower event id can commit
AFTER one with a higher id. A consumer reading by id would skip it forever.

## Decision
Writers insert into `events` and mark `published=false` in the same
transaction. An advisory-locked relay copies newly committed events into
`outbox` with a gapless, commit-ordered `position`. Consumers read `outbox`
by position. `head_id()` triggers the relay so lag is measurable.

## Alternatives rejected
- *Read events by id with a "wait for gaps" heuristic.* Unbounded wait; a
  rolled-back id is a permanent gap.
- *Logical decoding / CDC.* Correct but ties the service to PostgreSQL
  replication configuration; out of scope for a pilot.

## Consequences
One extra table and an O(pending) relay pass; `outbox` grows with the log
(both are source-of-record adjacent and are not pruned).
