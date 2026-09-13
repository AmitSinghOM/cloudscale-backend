# ADR-0002: Event log as sole source of record

**Status:** Accepted  **Date:** 2026-08 (reconstructed 2026-09-13)
**Enforced by:** RUNBOOK R7 (rebuild procedure); `processed_events` claim
tests; retention job never touches `events`

## Context
Balances, idempotency records, outbox rows and offsets can all be
recomputed from an ordered, immutable event history. Nothing can recompute
the history.

## Decision
`events` (with `event_envelopes` for identity/trace metadata) is the only
table that is backed up as truth. Every other table is derived and has a
documented rebuild. Events are JSON with an explicit `schema_version`.

## Alternatives rejected
- *Balances as truth with an audit trail.* Loses the ability to answer new
  questions about the past and to replay after a projection bug.
- *Binary/pickled events.* Unreadable outside Python; hostile to a
  seven-year horizon.

## Consequences
Replay must be exactly-once (`processed_events`), and event shapes must
evolve compatibly — which is why ADR-0009 exists.
