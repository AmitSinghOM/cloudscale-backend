# ADR-0006: Consumer HA via session advisory lock

**Status:** Accepted  **Date:** 2026-09-11
**Enforced by:** `tests/unit/entrypoints/test_consumer_ha.py`; two-process
SIGKILL run recorded in PR #7; soak harness failover injection

## Context
The projection consumer must not be a single point of failure, but two
consumers applying the same events would race the projection.

## Decision
Each consumer replica holds a dedicated connection and takes a
PostgreSQL *session-level* advisory lock keyed by consumer name. Exactly
one drains; others poll `try_acquire`. The lock dies with the connection,
so a crashed leader is replaced within one poll interval with no timeout
tuning and no split-brain window. `held()` pings the connection so a
partitioned leader stops draining.

## Alternatives rejected
- *Lease row with TTL.* Requires clock agreement and a tuned timeout; the
  window between crash and expiry is either split-brain or downtime.
- *External coordinator (ZooKeeper/etcd).* A new dependency to run for a
  problem PostgreSQL already solves.
- *Partitioned consumers.* The eventual scale answer, but adds ordering
  complexity; deferred until a single leader's throughput is the limit.

## Consequences
One leader per consumer name is a throughput ceiling (documented to
customers). The SQLite tier uses `NoLease` (single host by construction).
