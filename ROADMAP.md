# cloudscale-backend — Build Roadmap

Built in weekend-sized phases. Each phase ends in a tagged release, a short demo, and (where it fits) a blog post. **Do not start until AgentOS Phase 1 is shipped.**

> Rule: ship small, iterate visibly. Earn scope by finishing.

## Phase 0 — Command/query split  ·  ~2 wknds  ·  ✅ done
**Goal:** Write path appends events; read path serves projections

- [x] Append-only in-memory `EventStore` (per-stream 1-based seq, immutable, locked)
- [x] `CommandHandler` — Deposit/Withdraw account aggregate, no-overdraft via replay
- [x] `BalanceProjection` — pure fold, rebuildable from the log
- [x] Tests green (6)

## Phase 1 — Durable event log + idempotent consumer  ·  ✅ tier realized (SQLite)
**Goal:** Events to Kafka; consumer builds read models; idempotent consume

**Status:** The *durable-log + idempotent-consumer tier* is built and tested.
Kafka + PostgreSQL themselves remain **deferred** (heavy deps; memory-tight
host). SQLite is the stdlib-only realization of the same tier — same guarantees
(durability, ordering, exactly-once projection effect), swappable later behind
the existing store seam. Not yet done: Kafka broker, Postgres read models,
multi-consumer partitioning.

- [x] `SqliteEventStore` — ACID append (durable across restart), UNIQUE(stream, seq)
      for per-stream ordering, global `id` for total order, stable `event_id`
- [x] `IdempotentProjectionStore` — durable read model; dedupe by `event_id`
      inside the apply transaction → exactly-once effect under at-least-once delivery
- [x] `run_consumer` — resumes from a persisted offset; safe to re-run
- [x] Tests prove durability across simulated restart, idempotency under
      duplicate/replay, and ordering (11 new; 17 in the legacy suite, preserved
      exactly by the Milestone-1 gate below)
- [x] PostgreSQL tier (2026-09-08): `cloudscale/adapters/postgres/` —
      `PostgresEventStore` + `PostgresProjectionStore` implement the same
      contracts behind the same seams (optimistic append, exactly-once apply,
      DLQ + redrive), verified against live PostgreSQL 17.10 by PG-gated
      tests (skip when no server; throwaway DB created and dropped per run).
      Documented caveat: `read_all` id-order vs commit-order skew under
      multi-writer; single-writer scope today, transactional outbox is the
      multi-writer fix.
- [ ] Kafka swap — **deferred by decision (2026-09-08)**, not by neglect:
      this host has no Docker and no broker, and an untestable adapter would
      violate the evidence rule that everything shipped is verified. The
      consumer already speaks a transport-agnostic `EventFeed` protocol;
      revisit when a broker (or Docker for testcontainers, already pinned in
      dev deps) is available.

## Milestone 1 — Local correctness core (`cloudscale/` package)  ·  ✅ verified
**Goal:** Production-shaped domain/application/adapters architecture with the
same durability + idempotency guarantees, provable locally and claim-safe

- [x] Hexagonal `cloudscale/` package: typed domain (aggregate, commands,
      versioned event envelopes, results), application ports (Protocols) +
      command/query services, SQLite adapters
- [x] Command normalization + idempotency keyed on `command_id` with a
      canonical-payload digest (retry-safe, trace-independent)
- [x] Compat adapters keep the legacy `cqrs/` API green (17/17, count-locked)
- [x] 9 Hypothesis property suites (valid/invalid commands, optimistic append,
      concurrent no-overdraft, envelope stability, command idempotency,
      correlation identity, schema versions, SQLite compatibility; fixed seed)
- [x] Architecture test enforces dependency boundaries; process/failpoint
      harness self-test
- [x] `scripts/verify_milestone.py 1` — deterministic gate, revision-bound
      evidence under `evidence/<git-sha>/milestone-1/` (JUnit XML + JSON)
- [x] Suite: **122 green** (17 legacy + 105 additions)
- [ ] Outstanding gates (recorded in evidence, block any production claim):
      acceptance criteria 5–19, sustained 1000 rps, command p99 ≤ 300 ms,
      query p99 ≤ 100 ms, projection lag ≤ 1 s, 99.9% availability, fresh
      revision-bound release evidence. Excluded scope: HTTP/network, Kafka
      delivery, PostgreSQL tier, authn/z, deployment/operability.

## Phase 2 — Resilience  ·  ~1 wknd  ·  ✅ done (v0.2.0)
**Goal:** Circuit breakers, retries, DLQ for poison messages

- [x] `cloudscale/resilience/` — pure, stdlib-only primitives:
      `RetryPolicy` + `call_with_retry` (capped exponential backoff, full
      jitter, injectable sleep/random) and `CircuitBreaker` (closed → open →
      half-open single-probe, injectable clock; only designated *transient*
      error types count as failures — a deterministic rejection proves the
      downstream is healthy)
- [x] `DeadLetteringProjectionStore` — durable `dead_letters` table in the
      same SQLite DB; dead-lettering claims the `event_id` in
      `processed_events`, records the letter, and advances the offset in one
      transaction → poison events never wedge the log and replays are
      absorbed as duplicates (same exactly-once mechanism as the happy path)
- [x] `ResilientConsumer` (`cloudscale/processes/`) — breaker wraps each
      apply attempt inside the retry loop; poison → DLQ on first attempt,
      retry exhaustion → DLQ with attempt count, open circuit → halt with
      offset untouched (no loss, no false dead-letter)
- [x] Deterministic tests: 20 new (fake clocks, recorded sleeps, scripted
      failures, end-to-end over the real SQLite log) — suite 142 green
- [x] DLQ redrive tooling — `redrive`/`redrive_all` on the store (re-apply +
      letter removal in one transaction; failed redrive re-parks with an
      incremented attempt count; `processed_events` claim kept so replays
      still dedupe) + `scripts/dlq.py` CLI (list / show / redrive, exit codes
      for automation)
- [x] Tagged release `cloudscale-backend/v0.2.0` + README status update
- [ ] Wire breaker/retry around the command path — deferred to the HTTP tier
      (no remote downstream exists on the command side yet; the primitives
      are ready)

## Phase 3 — Load + observe  ·  ~1 wknd  ·  ✅ done (v0.3.0)
**Goal:** Load test, capture p99 / throughput, trace the hot path, write up bottleneck+fix

- [x] `scripts/load_and_observe.py` — stdlib load harness over the real
      file-backed pipeline (commands → durable log → resilient consumer →
      queries); p50/p95/p99 + throughput per segment; revision-bound JSON
      report under `evidence/<sha>/phase-3-load/`; honest-scope statement
      (single process/thread, local disk, no HTTP — baseline, not benchmark)
- [x] Instrumented bottleneck candidate: the no-overdraft rule replays the
      full stream per withdraw (O(n) hot path) — the harness samples latency
      at increasing stream depth to quantify the degradation
- [x] Baseline run committed as evidence (74710a8: hot withdraws 313/s,
      latency linear in depth)
- [x] Write up bottleneck + fix and implement it, with before/after runs —
      `docs/phase-3-bottleneck-withdraw-guard.md`: memoized fold +
      incremental `read_after` catch-up; 313/s → 9,246/s (29.6×), withdraw
      latency flat in stream depth, decision-identity to the replay
      implementation proven by test
- [x] Trace the hot path — OpenTelemetry spans over command → append →
      consume → project (`cloudscale/adapters/telemetry.py`, harness
      `--trace`, in-memory exporter; traced evidence shows the durable
      append dominates post-fix: 0.12 of 0.16 ms mean per command, guard
      read 0.01 ms)
- [x] Tagged release `cloudscale-backend/v0.3.0`

## Phase 4 — HTTP tier  ·  ~2 wknds  ·  ✅ done (v0.4.0)
**Goal:** Expose the typed command/query paths over FastAPI; close the
Milestone 1 network-scope gates

The deps are already pinned (fastapi, uvicorn, pydantic-settings, pyjwt,
opentelemetry-instrumentation-fastapi). Build order:

- [x] App skeleton (`cloudscale/entrypoints/http/`): FastAPI app factory
      over injected collaborators, fail-closed pydantic-settings config
      (no default JWT secret), storage-tier metadata on `/v1/health`
- [x] Auth: JWT bearer (pyjwt, HS256) — expired / unsigned / wrong-issuer /
      subject-less tokens all 401; query auth relaxable by explicit setting,
      writes will always authenticate
- [x] Query endpoint: `GET /v1/accounts/{id}/balance` via `QueryService` +
      `StoreProjectionReader` (works over both storage tiers); 404 when
      absent, 400 on domain-invalid ids, explicit `consistency: eventual`
      field (projection-lag measurement still to come)
- [x] **Concrete `CommandUnitOfWork` adapter (SQLite)** —
      `SqliteCommandUnitOfWork`: one BEGIN IMMEDIATE transaction covers
      stored-result lookup, stream fold, expected-version gate, aggregate
      `decide`, append, and result persistence. Equal-hash replay returns
      the original persisted result byte-for-byte; hash conflict → 409
      without overwriting the original; deterministic rejections persisted
      without appending; mid-transaction failure rolls back everything.
      Events land in the same `events` table the consumer polls; full
      envelope identity retained in `event_envelopes`.
- [x] Postgres `CommandUnitOfWork` — same contract, cross-process races
      arbitrated by UNIQUE constraints + bounded retry; PG-gated tests
      include two real cross-instance races (same-version, same-command-id)
- [x] Command endpoint: `POST /v1/accounts/{id}/commands` through
      `CommandService` with client-supplied `command_id`; HTTP status taken
      from the persisted transport-neutral result (201 / 409 replay-conflict
      / 409 version / 422 funds / 400 domain); writes authenticate
      unconditionally even when query auth is relaxed; end-to-end test
      proves POST → unit of work → log → consumer → GET (including the
      read-model trailing the log before the consumer runs)
- [x] Wire Phase 2 resilience around the command path — breaker (transient
      errors only; deterministic rejections never trip it) + retry behind
      the endpoint; open circuit / exhausted budget → 503 + Retry-After,
      safe to retry with the same command_id
- [x] OTel FastAPI instrumentation via optional ``tracer_provider`` on
      ``create_app`` (``configure_in_memory_provider`` in the telemetry
      adapter)
- [x] HTTP gate run (`scripts/http_gate_run.py`): real uvicorn + real
      consumer process + authenticated load + projection-lag probes.
      **All four evaluable Milestone 1 gates PASS** on the SQLite tier
      (10 s window, localhost): 1,431 rps sustained (gate 1,000), command
      p99 66 ms (gate 300), query p99 13 ms (gate 100), max projection lag
      0.103 s (gate 1 s). The 30-day availability gate is recorded as
      not_evaluated — a bench run cannot honestly claim it. Evidence under
      `evidence/<sha>/phase-4-http-gates/`.
- [x] Consumer as a real process (`cloudscale/entrypoints/consumer_loop`),
      lag measured, not simulated
- [x] DoD: suite green (204), gate-run evidence recorded, README status
      updated, tagged `cloudscale-backend/v0.4.0`

## Phase 5 — Production readiness  ·  ~4–6 wknds  ·  🚧 started
**Goal:** Close the CTO / staff-security review gaps so the service can carry
customer traffic. Source: the 2026-09-10 review (six blocking findings).

### Done (2026-09-10)
- [x] **Authorization** (CRITICAL #1): claims-based, default-deny — `accounts`
      claim names permitted accounts, `accounts:admin` scope grants all;
      403 otherwise, on commands and queries alike
- [x] **Observability** (CRITICAL #2): JSON-lines structured logs, request log
      with latency + subject, append-only audit record per command decision
      (never the token), Prometheus registry at `/metrics`
- [x] **Abuse controls** (HIGH #3): per-subject token-bucket rate limit
      (429 + Retry-After), body-size cap (413), CORS closed by default
- [x] **Delivery** (HIGH #6, part): multi-stage non-root Dockerfile with
      hash-pinned install and health check; GitHub Actions CI running the
      full gate against a PostgreSQL service container (asserts the PG tests
      did not skip) and building the image with a fail-closed startup smoke
- [x] Hardening: fixed 401 message, optional `aud` enforcement,
      `extra="forbid"` on command requests, lifespan closes storage,
      README architecture reflects what is built

### Remaining, in priority order
- [ ] **Account ownership registry** — authorization today trusts the token
      issuer to name accounts. Add `POST /v1/accounts` that creates an
      account bound to the caller's subject (persisted alongside the log),
      and authorize against that record; keep claims as the admin/service
      path. Closes the gap between "token says so" and "the system knows".
- [ ] **Connection pooling + transactional outbox** (HIGH #4) — replace the
      single-connection-behind-a-lock adapters with `psycopg_pool`
      (the unit-of-work storage protocol needs a per-call connection handle),
      then drain the log through an outbox in commit order so the documented
      multi-writer `read_all` id-skew stops being a caveat. Unlocks multiple
      uvicorn workers and horizontal scale.
- [ ] **Shared rate limiter** — the in-process bucket bounds one replica;
      move to a shared store (PostgreSQL or Redis) once there are replicas.
- [ ] **Identity** (HIGH #5) — OIDC/JWKS verification (RS256/ES256,
      key rotation) as an alternative to the HS256 shared secret; short
      token lifetimes plus a revocation list for the admin scope.
- [ ] **Migrations** (HIGH #6, rest) — Alembic-managed schema instead of
      `CREATE TABLE IF NOT EXISTS` at startup; documented backup/restore.
- [ ] **Consumer HA** — leader election or partitioned ownership so the
      projection consumer is not a single point of failure; export lag as a
      metric and alert on it. Prerequisite for honestly evaluating the
      Milestone 1 availability gate.
- [ ] **Runbooks + SLOs** — on-call docs for DLQ redrive, circuit-open
      recovery, consumer restart; SLOs derived from the gate-run numbers.
- [ ] Tagged release v0.5.0 with a fresh dual-tier gate run

## Definition of done (every phase)
1. Tests pass, CI green.
2. README status updated.
3. Tagged release.
4. Blog post drafted if it maps to one.
