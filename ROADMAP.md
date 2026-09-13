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

## Phase 5 — Production readiness  ·  ~4–6 wknds  ·  ✅ done (v0.5.0)
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
- [x] **Account ownership registry** (2026-09-11) — `AccountRegistry` port
      with SQLite and Postgres realizations; `POST /v1/accounts` binds an
      account to the caller's subject (201 created / 200 idempotent for the
      owner / 409 taken); authorization now grants access by admin scope,
      by claims, OR by registered ownership — everything else, including
      unregistered accounts, stays 403. Registrations are audit-logged.
      Cross-connection registration race tested on PG (exactly one winner).
- [x] **Connection pooling + transactional outbox** (2026-09-11) — all four
      PG adapters check connections out of a `psycopg_pool` per operation
      (no process-wide lock; `CLOUDSCALE_PG_POOL_MAX`, default 4); the unit
      of work hands the decision core a per-call bound-connection storage
      view so fold/append/persist stay atomic. `read_all` now reads by a
      gapless, commit-ordered outbox `position` assigned by an
      advisory-locked relay (anti-join over unpublished events), so a
      late-committing smaller id is delivered instead of skipped —
      regression-locked by a held-open-transaction test. SQLite tier is
      unchanged (single writer, commit order = id order).
- [x] **Shared rate limiter** (2026-09-11) — `PostgresRateLimiter`: one
      atomic upsert per request using the database clock, so every replica
      draws from a single per-subject budget; `CLOUDSCALE_RATE_LIMIT_BACKEND=
      postgres` on the PG tier. Two-replica shared-budget test with
      server-clock refill.
- [x] **Identity** (2026-09-11) — OIDC/JWKS verification (RS256/ES256 via
      `PyJWKClient`, `kid`-based key rotation without restarts) as an
      alternative to the HS256 shared secret; settings require exactly one
      mode and refuse algorithm confusion at configuration time; `iat`
      required and lifetime capped (`CLOUDSCALE_JWT_MAX_LIFETIME_SECONDS`,
      default 1 h); admin-scoped tokens must carry `jti` and honor a
      revocation list (`CLOUDSCALE_JWT_REVOKED_JTIS`). Tested against a
      locally generated RSA JWKS: accept, rotate, reject unknown key,
      reject HS256 in JWKS mode, lifetime, revocation.
- [x] **Migrations** (2026-09-11) — Alembic with the schema defined ONCE
      (`adapters/postgres/schema.py`) and consumed by both migration
      `0001_initial` and the adapters' dev-mode auto-create; a PG-gated test
      asserts the two paths yield identical `information_schema`.
      `CLOUDSCALE_PG_SCHEMA=migrations` (production) makes adapters create
      nothing and refuse to start unless Alembic is at the required
      revision; `python -m cloudscale.entrypoints.migrate` applies. Dockerfile
      documents the sequence. Backup/restore guidance → runbooks item.
- [x] **Consumer HA** (2026-09-11) — leader election via a PostgreSQL
      *session-level* advisory lock on a dedicated connection: exactly one
      consumer per name drains, standbys poll; a dead leader's lock drops
      with its connection so failover needs no timeouts and has no
      split-brain window. Consumer process exports Prometheus gauges
      (`is_leader`, `lag_events`, `last_drain_timestamp`) and counters
      (applied, dead-lettered, halts). Verified: two real processes, one
      leader, SIGKILL → standby leads and drains backlog within 1 s.
- [x] **Runbooks + SLOs** (2026-09-11) — `docs/RUNBOOK.md` (8 procedures:
      migration mismatch, lag, DLQ growth/redrive, 503s, 429s, key rotation
      and admin revocation, backup/restore with what is derived vs. source of
      record, failover expectations) and `docs/SLO.md` (SLIs on exported
      metric names, targets with headroom over gate evidence, 43-min error
      budget, multi-window burn-rate + latency + consumer alert rules in
      PromQL; availability honestly marked target-not-yet-demonstrated).
- [x] Tagged release v0.5.0 (2026-09-12) with a fresh gate run. SQLite tier:
      all four evaluable gates pass (1,394 rps, command p99 72.6 ms, query
      p99 9.3 ms, lag 0.077 s), evidence committed. **PostgreSQL tier: the
      1,000 rps gate does not pass on one uvicorn worker** — 852 rps, with
      command p99 31–51 ms and lag < 0.1 s passing.
      *Correction (2026-09-13):* the release-day attribution to host
      contention was **wrong**. Two runs on different days under different
      load agreed to 0.05 % (851.9 / 852.3 rps), which is a structural
      ceiling, not noise. Profiled: every PG primitive is < 1 ms (SELECT
      0.13 ms, append 0.56 ms); single-client HTTP latency on the PG tier is
      *better* than SQLite (command 1.12 ms vs 1.50 ms). The gap exists only
      under 12-way concurrency: one Python process saturates its GIL and the
      PG request path does more Python-side work per request (pool checkout,
      dict-row decoding, outbox write) than the in-process SQLite path.
      Raising `CLOUDSCALE_PG_POOL_MAX` 4→16 cut command p99 51→35 ms but
      added only 20 rps, confirming the process, not the pool, is the limit.
      The tier's design answer is horizontal — the harness now takes
      `--server-workers N` and records it in the report so evidence can
      never mislabel the deployment shape. A 2-worker run reached 896 rps
      on a host at load 9.7/10 cores and is not committed. Follow-up:
      `--storage postgres --server-workers 2` on a quiet host. Last clean
      1-worker PG pass: 1,218 rps on 5c75f99 (pre-pooling/outbox).

## Definition of done (every phase)
1. Tests pass, CI green.
2. README status updated.
3. Tagged release.
4. Blog post drafted if it maps to one.
