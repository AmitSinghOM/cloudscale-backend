# cloudscale-backend

**A production-shaped distributed backend demonstrating CQRS, event sourcing, async processing, and resilience patterns at scale.**

> **Status: Phases 0–3 done (v0.3.0); Milestone 1 (local correctness) verified.** The original
> `cqrs/` command/query split ships with an in-memory event log, and the
> durable event log + idempotent consumer are built and tested on SQLite
> (stdlib-only). Milestone 1 added a typed hexagonal `cloudscale/` package
> (domain / application / adapters) behind the same guarantees, verified by a
> test suite with 9 Hypothesis property suites and a revision-bound evidence
> gate (`scripts/verify_milestone.py`). Phase 2 adds `cloudscale/resilience/`
> (retry with backoff + circuit breaker), a dead-letter queue wired into a
> resilient consumer, and DLQ redrive tooling (`scripts/dlq.py`). Phase 3
> measured the pipeline (`scripts/load_and_observe.py`), found and fixed the
> withdraw guard's O(n) replay (29.6× hot-path throughput; see
> `docs/phase-3-bottleneck-withdraw-guard.md`), and traces the hot path with
> OpenTelemetry (`--trace`). The read/write tiers now also have a PostgreSQL
> realization (`cloudscale/adapters/postgres/`, verified against live PG by
> gated tests); Phase 4 (HTTP tier) is planned in the ROADMAP. HTTP and Kafka
> remain deferred behind the ports — see [ROADMAP.md](./ROADMAP.md).

## Why this exists

A reference backend that does the unglamorous things right: separates reads from writes, sources state from events, isolates failures with circuit breakers, and never silently drops work. The point is to *operate* it under load and show the numbers.

## Architecture (planned)

```
API (FastAPI) → Command side (writes → event log) → Kafka → Projections (read models) ; Query side reads projections
```

## Components

- CQRS: separate command and query paths
- Event sourcing with Kafka as the log
- Read-model projections in PostgreSQL
- Circuit breakers around downstream calls
- Dead-letter queue for poison messages
- Idempotent consumers + OpenTelemetry tracing

## Repository layout

```
cloudscale/            Milestone-1 hexagonal package
  domain/              Account aggregate, commands, event envelopes, results, errors
  application/         Typed ports (Protocols) + command/query services
  resilience/          Pure retry + circuit-breaker primitives (stdlib-only)
  adapters/            SQLite adapters + compat shims over the legacy cqrs/ stores
  processes/           Resilient consumer (retry + breaker + dead-letter queue)
cqrs/                  Original Phase-0/1 implementation (kept green, 17 tests)
tests/
  unit/ properties/    Domain units + 9 Hypothesis property suites (fixed seed)
  architecture/        Dependency-boundary enforcement (domain imports nothing outward)
  compat/ failure/     Legacy-API compatibility + process/failpoint harness self-test
  milestones/          Milestone-1 contract (scope, gates, claim-safety)
scripts/verify_milestone.py   Deterministic gate; writes evidence/<git-sha>/milestone-1/
```

## Verifying

```
make install-dev   # hash-pinned lockfile into .venv (Python 3.12)
make check         # ruff format-check + lint, mypy, pytest (122 tests)
.venv/bin/python scripts/verify_milestone.py 1   # revision-bound evidence gate
```

Milestone 1 is deliberately **local-only**: HTTP/network behavior, Kafka
delivery, the PostgreSQL production tier, auth, and load/availability gates
are excluded scope and recorded as outstanding gates in the evidence payload —
no production-readiness claim is made or permitted by the contract tests.

## Tech stack

Python · FastAPI · Kafka · PostgreSQL · Redis · OpenTelemetry · Prometheus/Grafana · k6 (load)

See [ROADMAP.md](./ROADMAP.md) for the phased build plan.
