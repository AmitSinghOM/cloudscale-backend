# cloudscale-backend

**A production-shaped distributed backend demonstrating CQRS, event sourcing, async processing, and resilience patterns at scale.**

> **Status: Phases 0–5 done plus production-readiness hardening (v0.5.1);
> Milestone 1 (local correctness) verified;
> all four evaluable Milestone 1 network gates passing on the SQLite tier.** The original
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
> gated tests). Phase 4 shipped the HTTP tier: authenticated
> command/query endpoints over the typed application layer with an idempotent
> unit of work on both tiers, breaker+retry on the command path, optional
> OTel server spans, and a real-deployment gate run
> (`scripts/http_gate_run.py`) in which **all four evaluable Milestone 1
> network gates pass** on the SQLite tier (1,431 rps sustained, command p99
> 66 ms, query p99 13 ms, projection lag ≤ 0.103 s; 30-day availability
> honestly not_evaluated). Kafka remains deferred behind the ports — see
> [ROADMAP.md](./ROADMAP.md).

## Why this exists

A reference backend that does the unglamorous things right: separates reads from writes, sources state from events, isolates failures with circuit breakers, and never silently drops work. The point is to *operate* it under load and show the numbers.

## Architecture (as built)

```
Client ──JWT──▶ FastAPI (authn · claims-based authz · rate limit · audit log · metrics)
                  │ POST /v1/accounts/{id}/commands          GET /v1/accounts/{id}/balance
                  ▼                                                     ▲
        CommandService ─▶ CommandUnitOfWork (idempotent, atomic)       QueryService
                  │  decide → append event → persist result             │
                  ▼                                                     │
          Durable event log ──▶ ResilientConsumer (retry · breaker · DLQ) ──▶ Projection
          (SQLite | PostgreSQL)     separate process                        (SQLite | PostgreSQL)
```

Kafka as the log transport is **deferred by decision** (no broker available to
verify against); the consumer speaks a transport-agnostic `EventFeed` protocol,
so the swap is wiring, not redesign.

## Components

- CQRS: separate command and query paths through a typed application layer
- Event sourcing on a durable, totally ordered log (SQLite and PostgreSQL realizations)
- Read-model projections with exactly-once effect under at-least-once delivery
- Circuit breakers + bounded retry on the command path and in the consumer
- Dead-letter queue with redrive tooling for poison messages
- JWT authentication; authorization by registered account ownership (`POST /v1/accounts`), token claims, or admin scope — default deny; per-subject rate limiting
- Structured JSON logs, append-only command audit log, Prometheus metrics, OpenTelemetry tracing

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
make check         # ruff format-check + lint, mypy, pytest
.venv/bin/python scripts/verify_milestone.py 1   # revision-bound evidence gate
```

## Running on PostgreSQL (production shape)

```
export CLOUDSCALE_PG_DSN=postgresql://user:pass@host/db
python -m cloudscale.entrypoints.migrate                 # Alembic upgrade head, once per release
export CLOUDSCALE_STORAGE=postgres CLOUDSCALE_PG_SCHEMA=migrations \
       CLOUDSCALE_RATE_LIMIT_BACKEND=postgres CLOUDSCALE_JWT_JWKS_URL=https://issuer/.well-known/jwks.json
uvicorn --factory cloudscale.entrypoints.http.main:build_app   # any number of replicas
python -m cloudscale.entrypoints.consumer_loop                 # projection consumer
```

In `migrations` mode the processes never create schema and refuse to start
unless the database is at the Alembic revision this build requires. The
schema is defined once (`cloudscale/adapters/postgres/schema.py`) and a test
asserts that migrating and dev-mode auto-creation produce identical tables.

Milestone 1 is deliberately **local-only**: HTTP/network behavior, Kafka
delivery, the PostgreSQL production tier, auth, and load/availability gates
are excluded scope and recorded as outstanding gates in the evidence payload —
no production-readiness claim is made or permitted by the contract tests.

## Tech stack

Python · FastAPI · Kafka · PostgreSQL · Redis · OpenTelemetry · Prometheus/Grafana · k6 (load)

See [ROADMAP.md](./ROADMAP.md) for the phased build plan.
