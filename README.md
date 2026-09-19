# cloudscale-backend

**A production-shaped distributed backend demonstrating CQRS, event sourcing, async processing, and resilience patterns at scale.**

> **Status: Phases 0–5 done, hardened, with a seven-year longevity structure and
> double-entry transfers (v0.7.0);
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

Double-entry transfers between accounts (`POST /v1/accounts/{id}/transfers`,
ADR-0011) commit both legs in one transaction with the same idempotency and
concurrency guarantees as single-account commands.

## Quickstart (60 seconds, no infrastructure)

```bash
git clone https://github.com/AmitSinghOM/cloudscale-backend && cd cloudscale-backend
make install-dev          # Python 3.12 or 3.13; hash-verified install into .venv
make dev                  # API + projection consumer on http://127.0.0.1:8000 (SQLite tier)
make token ARGS=--curl    # prints a ready-to-run deposit; paste it
curl -s http://127.0.0.1:8000/v1/accounts/demo/balance \
  -H "Authorization: Bearer $(make -s token)"          # → {"account_id":"demo","balance":100,...}
make stop                 # data stays in .dev/; delete the directory to reset
```

Interactive API docs at `/docs`, readiness at `/v1/ready`, metrics at
`/metrics`. `examples/python_client.py` is a copy-paste client that shows the
four things integrators get wrong (retry with the same `command_id`, handle
409 by re-reading the version, wait for the projection, treat a transfer as
two postings with the version guard on the source only) and runs against
`make dev`. `make check` runs the full gate (format · lint · types · the
whole test suite) in well under a minute. The dev secret is fixed and local-only; the
production shape (PostgreSQL, OIDC/JWKS, migrations) is
[below](#running-on-postgresql-production-shape).

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

Full write path, read path, failure behaviour, deployment topology and a
"where to change what" table: [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md).

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

## Governance (how this stays safe to change)

| Document | Purpose |
|---|---|
| `docs/ARCHITECTURE.md` | Write/read paths, failure behaviour, topology, and which test proves each guarantee |
| `docs/LONGEVITY.md` | The charter: what keeps this maintainable through 2033, marked enforced vs. policy |
| `docs/adr/` | Architecture Decision Records — the *why* behind every load-bearing decision |
| `CONTRIBUTING.md` | Definition of done, gates, evidence rule |
| `AGENTS.md` | Contract for language-model contributors: same gates, invariants never weakened, humans own the irreversible |
| `SECURITY.md` · `docs/THREAT_MODEL.md` | Disclosure policy; STRIDE model with dated accepted risks |
| `docs/RUNBOOK.md` · `docs/SLO.md` | Operations procedures; SLIs, targets, alert rules |
| `docs/CONFIGURATION.md` | Every environment variable, default, and production value — completeness enforced by a test |
| `docs/API_ERRORS.md` | Every status and error code a client can receive, with the correct client action — completeness enforced by a test |
| `docs/openapi.json` | The HTTP contract, committed; a test fails the build if the running app's schema drifts from it. Generate clients from this file |
| `CHANGELOG.md` | Operator-facing history per release |

The ADR index, changelog structure and presence of these files are checked
by `tests/architecture/test_governance_docs.py` — governance is part of the
build, not a wish.

## Verifying

```
make install-dev   # hash-pinned lockfile into .venv (Python 3.12 or 3.13)
make check         # ruff format-check + lint, mypy, pytest
.venv/bin/python scripts/verify_milestone.py 1   # revision-bound evidence gate
```

## Running on PostgreSQL (production shape)

One command, production mode (Alembic owns the schema; the app creates nothing):

```bash
docker compose up --build            # PostgreSQL 17 → migrate → api :8000 + consumer (metrics :9100)
make token ARGS=--curl               # same local dev secret as the compose file; paste the curl
docker compose down -v               # reset
```

CI runs exactly this stack on every pull request and asserts `/v1/ready`,
an accepted deposit, and the balance read model — so the compose file is
tested, not decorative. Manual equivalent:

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
