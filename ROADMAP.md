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
- [ ] Swap SQLite log for Kafka; swap read model for PostgreSQL (deferred)

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

## Phase 2 — Resilience  ·  ~1 wknd  ·  🚧 core landed
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
- [ ] DLQ redrive tooling (inspect + requeue parked events)
- [ ] Wire breaker/retry around the command path (needs the HTTP tier)
- [ ] Tagged release + README status update + demo

## Phase 3 — Load + observe  ·  ~1 wknd
**Goal:** Load test, capture p99 / throughput, trace the hot path, write up bottleneck+fix

- [ ] (break into tasks when you start this phase)

## Definition of done (every phase)
1. Tests pass, CI green.
2. README status updated.
3. Tagged release.
4. Blog post drafted if it maps to one.
