# Architecture

The README shows the shape; this page shows *why each box exists*, what it
guarantees, and which test proves it. Read it before changing anything that
crosses a line in a diagram. Decisions are in `docs/adr/`.

## Layers (hexagonal, enforced)

```
┌──────────────────────────── entrypoints ────────────────────────────┐
│  http/ (FastAPI · auth · limits · observability)   consumer_loop    │
│  migrate · retention                                                │
├──────────────────────────── adapters ───────────────────────────────┤
│  sqlite_compat/            postgres/            projection_readers  │
│  (single host)             (pooled, HA, outbox)                     │
├──────────────────────────── application ────────────────────────────┤
│  ports.py (Protocols) · command_execution (decision core)           │
│  command_service · query_service                                    │
├──────────────────────────── domain ─────────────────────────────────┤
│  account (fold) · commands · events · results · errors · upcasting  │
└─────────────────────────────────────────────────────────────────────┘
```

Dependencies point **downward only**. `domain` and `application` import the
standard library and each other; nothing else. Enforced by
`tests/architecture/test_dependency_boundaries.py` (ADR-0001). The practical
payoff: PostgreSQL was added without touching the domain (ADR-0004).

## Write path

```
POST /v1/accounts/{id}/commands          POST /v1/accounts/{id}/transfers  (ADR-0011)
  │  ClientRateLimit (pre-auth, per IP) → BodySizeLimit → authenticate → authorize → per-subject limit
  ▼
CommandService.normalize ── command_id, correlation_id, issuer, subject
  ▼
CommandUnitOfWork.execute            ONE transaction:
  ├─ lookup command_results[command_id]  → hit: return stored result (idempotent replay)
  ├─ fold_stream(account) ─ upcast each stored event → AccountState   (ADR-0009)
  ├─ execute_command_decision ─ pure: (state, command) → accept | reject
  ├─ append event (stream, seq = expected_version+1)  ← UNIQUE(stream, seq) is the concurrency guard
  │    transfer: fold the target too, decide_transfer → debit + credit legs sharing
  │    transfer_id; append BOTH in ascending account order (no A→B/B→A deadlock)
  ├─ write event_envelope (identity, trace, schema_version)
  └─ persist command_result (postings: one per stream written)
  ▼
201 (first execution and replay, byte-for-byte) / 409 / 422 / 400   (docs/API_ERRORS.md)
```

| Guarantee | Mechanism | Proof |
|---|---|---|
| A retried `command_id` never double-applies | result row written in the same transaction as the event | `test_sqlite_command_uow.py`, `test_postgres_command_uow.py` |
| Two writers on one version → exactly one wins | `UNIQUE (stream, seq)`; loser gets 409 with `current_version` | `test_concurrent_same_version_writers_yield_one_accept_one_conflict` |
| Partial failure leaves nothing behind | single transaction; PostgreSQL autocommit trap fixed | savepoint / cross-connection visibility tests |
| Decision logic exists once | `application/command_execution.py`; both adapters delegate | contract tests run against both tiers |
| A transfer moves money or nothing | both legs appended and the result persisted in the same transaction; `expected_version` guards the source, `UNIQUE (stream, seq)` + fresh-fold retry guard the target | `test_transfer_appends_both_legs_atomically…`, `test_target_side_race_self_heals…` |
| Transfers conserve the total | debit and credit are the same amount; `apply` refuses a negative balance | `test_transfers_conserve_the_total_and_never_go_negative` (property) |
| Opposite-direction transfers never deadlock | legs appended in ascending account-id order | `test_opposite_direction_transfers_never_deadlock` (proven to raise `DeadlockDetected` without the ordering) |

## Read path and the log → projection pipeline

```
events (source of record) ──relay──▶ outbox (gapless, commit-ordered position)   [PostgreSQL]
events (id order)                                                                 [SQLite]
        │
        ▼  ResilientConsumer (leader only)
   for each event: upcast → projection.apply
        ├─ processed_events claim → duplicate? skip            (exactly-once effect)
        ├─ transient error → retry (3) → breaker → halt (offset NOT advanced)
        └─ deterministic error → dead_letters (offset advances; redrive later)
        ▼
   balances (account_id, balance, version) ◀── GET /v1/accounts/{id}/balance
```

| Guarantee | Mechanism | Proof |
|---|---|---|
| A late-committing event is never skipped | outbox relay assigns positions in commit order under an advisory lock (ADR-0005) | outbox skew regression test |
| Replay never double-counts | `processed_events` claim per event id, kept across redrive | resilient-consumer + redrive tests |
| Poison never wedges the log | dead-letter with offset advance; `UnknownSchemaVersionError` is poison | `test_poison_event_is_dead_lettered…`, `test_consumer_dead_letters_an_event_from_the_future…` |
| Exactly one consumer drains | session advisory lock per consumer name (ADR-0006) | lease tests; two-process SIGKILL run; soak failover 0.27 s |
| Reads are eventual and say so | `consistency: "eventual"` in the response; clients poll to `committed_version` | `examples/python_client.py` |
| …and eventual **per account** | a transfer's two legs are applied as two events; between them a reader may see the debit without the credit. Conservation holds in the log at every commit and in the projection at every quiescent point (ADR-0011) | `test_transfer_legs_project_into_both_balances_on_postgres` |

## Failure behaviour (what a client sees)

| Condition | HTTP tier | Consumer |
|---|---|---|
| Database down | breaker opens → 503 + `Retry-After`; `/v1/ready` → 503 | breaker halts; lag metric rises; no offset movement |
| Pool exhausted | 3 s bound → transient → 503 (RUNBOOK R4) | n/a (own connections) |
| Identity provider hung | 3 s JWKS fetch bound; cached kids unaffected | n/a |
| Schema not migrated (prod mode) | refuses to start; `/v1/ready` 503 | refuses to start |
| Event from a newer build (rollback) | 503 "schema newer than this build" | dead-letters it; redrive after redeploy |
| Leader consumer killed | unaffected | standby leads within one poll (≈ 20 ms + detection) |

## Deployment topology (PostgreSQL tier)

```
            ┌──────────── edge (TLS, volumetric limits) ────────────┐
            │                                                      │
   ┌────────▼────────┐   ┌─────────────────┐   ┌──────────────────┐
   │ api ×N          │   │ consumer ×N     │   │ retention (cron) │
   │ /v1/ready       │   │ 1 leader        │   │ daily            │
   └────────┬────────┘   └────────┬────────┘   └────────┬─────────┘
            └─────────────────────┼─────────────────────┘
                          ┌───────▼────────┐
                          │ PostgreSQL 17  │  schema owned by Alembic
                          └────────────────┘
   migrate (one-shot, before rollout)   ·   Prometheus scrapes /metrics + :9100
```

`compose.yaml` is this topology at N=1 and is exercised in CI. Assumptions the
service does not implement itself — TLS, secrets, log sink, `/metrics`
exposure — are RUNBOOK D1–D7.

## Performance shape

Single-process throughput is CPU-bound (GIL): ~1,400 rps on the in-process
SQLite tier, ~850 rps per worker on PostgreSQL; scale the PostgreSQL tier
horizontally (`--workers N` or N replicas). Command p99 ≈ 30–70 ms, query
p99 ≈ 10–25 ms, projection lag < 0.1 s in gate runs. Evidence per commit
under `evidence/<sha>/`; targets and alerts in `docs/SLO.md`.

## Where to change what

| I want to… | Touch | Also |
|---|---|---|
| Add a command type | `domain/commands.py`, `domain/account.py`, `application/command_execution.py` | ADR; `docs/API_ERRORS.md`; `docs/openapi.json` regenerates |
| Add an event type | `domain/events.py` (type, payload, `BALANCE_SIGN`), `domain/upcasting.py` (`CURRENT_SCHEMA_VERSION`), `adapters/compat.py`, a `tests/fixtures/events/<Type>.v1.json` | every balance projection reads `BALANCE_SIGN`; the corpus test fails until the fixture exists |
| Change an event's shape | bump `CURRENT_SCHEMA_VERSION`, register an upcaster, add the fixture | ADR-0009; the corpus test tells you what is missing |
| Change `AccountState` or how an existing event folds | bump `CURRENT_STATE_VERSION` in `domain/account.py` | ADR-0012; stale snapshots are discarded and refolded, never upcast |
| Add a table or column | `adapters/postgres/schema.py` **and** a new Alembic revision, bump `CURRENT_REVISION` | parity test asserts both paths match |
| Add a setting | `HttpSettings` or `os.environ` | `docs/CONFIGURATION.md` (test enforces) |
| Add a projection | implement `apply(event) -> bool` + `dead_letter(...)` | consumer name → own lease |
