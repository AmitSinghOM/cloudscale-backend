# CloudScale Backend — Operations Runbook

Every procedure below references real metric names, real CLI entrypoints, and
real behaviour verified by tests in this repository. Where a step cannot be
verified from inside the service (e.g. your alerting stack), it says so.

## Topology

| Process | Entrypoint | Scaling | Health signal |
|---|---|---|---|
| HTTP tier | `uvicorn --factory cloudscale.entrypoints.http.main:build_app` | N replicas (PG tier) | liveness `GET /v1/health`, **readiness `GET /v1/ready`**, `GET /metrics` |
| Consumer | `python -m cloudscale.entrypoints.consumer_loop` | N replicas, **one leader** | `:CLOUDSCALE_CONSUMER_METRICS_PORT/metrics` |
| Migrations | `python -m cloudscale.entrypoints.migrate` | once per release | exit code |

**Probes.** Point the orchestrator's *liveness* probe at `/v1/health` (process
up, never touches storage) and its *readiness* probe at `/v1/ready`, which
does a real storage round-trip and, in `CLOUDSCALE_PG_SCHEMA=migrations`
mode, re-verifies the Alembic revision. `503` from `/v1/ready` means: stop
routing here; a rollout whose replicas never go ready has a schema or
connectivity problem (see R1). Both probes and `/metrics` bypass the
client rate limiter.

**Abuse controls, in request order.** (1) pre-authentication per-client
budget `CLOUDSCALE_CLIENT_RATE_LIMIT_PER_MINUTE` (default 1,200/replica) —
bounds the cost of token verification for unauthenticated floods; set
`CLOUDSCALE_TRUST_PROXY_HEADERS=true` **only** behind a proxy you control
that overwrites `X-Forwarded-For`, otherwise clients can spoof out of it;
(2) body cap; (3) authentication; (4) per-subject budget
`CLOUDSCALE_RATE_LIMIT_PER_MINUTE` (shared across replicas with the
`postgres` backend).

## Deployment assumptions (non-negotiable for customer traffic)

The service deliberately does **not** implement these itself. Each is a
precondition; a deployment missing one is not production.

| # | Assumption | Why | How to verify |
|---|---|---|---|
| D1 | **TLS is terminated upstream** (load balancer / ingress / sidecar). The app speaks plain HTTP on its listen port and sets no HSTS or other browser security headers. | Bearer tokens in the clear = account takeover. | Listen port is not reachable from outside the private network; the public hostname serves only HTTPS. |
| D2 | **`/metrics` on both tiers is network-restricted** to the scraper. It is unauthenticated by design. | Leaks route names, traffic shape, and error rates. | `curl` from outside the cluster network returns a connection error, not a 200. |
| D3 | **Audit and request logs are shipped to an append-only sink** (WORM bucket, immutable log store). `cloudscale.audit` is structured JSON on stdout; it carries no tamper evidence of its own. | Non-repudiation of every command outcome and ownership change. | The sink denies delete/overwrite to every principal the deployment uses. |
| D4 | **Secrets arrive from a secrets manager**, injected into the process environment at start; never baked into images, compose files, or CI logs. Covers `CLOUDSCALE_JWT_SECRET`, `CLOUDSCALE_PG_DSN`. | Environment variables are visible to anyone who can exec into the container. | Image history and repo contain no secret values; rotation does not require a code change. |
| D5 | **Client budgets are enforced at the edge too.** The in-app pre-auth limiter bounds CPU per replica; it is not a volumetric defence. | Layer-3/4 floods never reach the application. | Edge rate limit and connection limits configured; `CLOUDSCALE_TRUST_PROXY_HEADERS=true` set **only** if the edge overwrites `X-Forwarded-For`. |
| D6 | **Retention job is scheduled.** `python -m cloudscale.entrypoints.retention` runs at least daily (see R9). | Idempotency records and limiter buckets otherwise grow without bound. | Last run's JSON report is recent and its exit code was 0. |
| D7 | **Multiple HTTP workers/replicas on the PostgreSQL tier.** One uvicorn worker is CPU-bound near 850 rps; throughput scales horizontally (`--workers N` or N replicas). | Single-process ceiling is structural (GIL), not tunable. | Load test at the deployment's replica count, not on one worker. |

## Retention (R9)

`python -m cloudscale.entrypoints.retention` prunes, in bounded batches:

- `command_results` older than `--command-results-days` (default 7). This is
  the **idempotency window**: a client retrying a `command_id` older than
  this is treated as a new command. Set it longer than any client's retry
  horizon, never shorter.
- `rate_limit_buckets` idle for longer than `--rate-limit-idle-seconds`
  (default 3600). A pruned bucket is recreated full on next use — identical
  to a fully refilled one, so no behaviour changes.
- `dead_letters` **only** when `--dead-letters-days` is given. They are
  evidence of a producer or infrastructure defect; the default keeps them.

Never pruned: the event log, `event_envelopes`, `processed_events`,
`accounts`. Event-stream snapshots are a separate ROADMAP item; until then
the practical ceiling is cold-start replay time for the longest account
stream (the memoized fold keeps the hot path O(1) once warm).

Schema note: retention added `command_results.created_at` (Alembic
`0002_command_results_created_at`; the SQLite tier upgrades a legacy file in
place on first open). Pre-existing rows are stamped at migration time —
treated as fresh — so a first retention run right after upgrading prunes
nothing.

Production PostgreSQL env: `CLOUDSCALE_STORAGE=postgres CLOUDSCALE_PG_DSN=…
CLOUDSCALE_PG_SCHEMA=migrations CLOUDSCALE_RATE_LIMIT_BACKEND=postgres` plus
exactly one of `CLOUDSCALE_JWT_SECRET` / `CLOUDSCALE_JWT_JWKS_URL`.

---

## R1 — Deploy refuses to start: `SchemaNotMigratedError`

**Symptom.** Server or consumer exits at startup with
`CLOUDSCALE_PG_SCHEMA=migrations but the database has no alembic_version…` or
`database is at Alembic revision 'X'; this build requires 'Y'`.

**Meaning.** Working as designed: this build will not run against a schema it
did not migrate.

**Action.**
1. `CLOUDSCALE_PG_DSN=… python -m cloudscale.entrypoints.migrate current` — see what the DB is at.
2. `python -m cloudscale.entrypoints.migrate upgrade` — apply to head.
3. Restart the processes. If the DB is *ahead* of the build (rolled back a
   deploy), either roll the DB back (`migrate downgrade <rev>`) after
   confirming no newer-schema data is needed, or redeploy the newer build.

**Never** set `CLOUDSCALE_PG_SCHEMA=auto` in production to "make it start".

---

## R2 — Projection lag rising / reads stale

**Signals.** `cloudscale_consumer_lag_events{consumer="balances"}` climbing;
`cloudscale_consumer_last_drain_timestamp_seconds` not advancing; clients see
`404`/stale balances after `201` commands.

**Diagnose, in order.**
1. **Is there a leader?** `sum(cloudscale_consumer_is_leader) == 1` expected.
   - `0`: no consumer holds the lease. Check consumer processes are running
     and can reach PostgreSQL. A crashed leader releases the lock instantly;
     if *all* replicas are down, start one.
   - `>1`: impossible by construction (session advisory lock) — treat as a
     metrics/labeling bug, not a data risk.
2. **Is the leader halting?** `rate(cloudscale_consumer_halts_total[5m]) > 0`
   means the consumer's circuit breaker is open — the projection database is
   returning transient errors. Nothing is lost; the offset does not advance
   while halted. Fix the DB (connections, disk, locks), the breaker
   half-opens automatically.
3. **Is it dead-lettering?** See R3.
4. **Is it just slow?** Lag falls when `applied_total` rate exceeds append
   rate. The consumer is one process per consumer name by design; if
   sustained append rate exceeds drain rate, the fix is batching in
   `PostgresProjectionStore.apply` (one transaction per event today), not
   more replicas.

---

## R3 — Dead-letter queue growing

**Signals.** `rate(cloudscale_consumer_events_dead_lettered_total[15m]) > 0`.

**Meaning.** Events failed typed validation or exhausted retries. They are
parked, the offset advanced past them, nothing is blocked. Balances for the
affected accounts are **behind** until redriven.

**Action.**
1. Inspect: `python scripts/dlq.py <projection-db-or-dsn> list` / `show <event_id>`.
2. Classify the `error_type`:
   - `InvalidAmountError`, `ValueError` on payload → a producer bug wrote a
     bad event. Fix the producer; the event itself may be unrecoverable.
   - `OperationalError` with `attempts == 3` → transient storage failure
     survived the retry budget. Redrive is expected to succeed.
3. Redrive: `python scripts/dlq.py <db> redrive <event_id>` or `--all`.
   Exit `1` means at least one letter failed again (attempts incremented,
   fresh error recorded); exit `2` means not found.
4. Redrive is exactly-once: the `processed_events` claim is kept, so a log
   replay after a successful redrive does not double-apply.

---

## R4 — Command path returning 503

**Signals.** `cloudscale_http_requests_total{route="/v1/accounts/{account_id}/commands",status="503"}`
rising; response `detail` says `circuit open` or `transient storage failure`.

**Meaning.** The write database is unhealthy. The HTTP breaker counts only
transient errors — domain rejections (409/422/400) never trip it.

**Action.** Fix the database. Clients may **safely retry with the same
`command_id`** (the response carries `Retry-After`): the unit of work is
idempotent, so a retry after partial failure returns the original result or
performs the command exactly once.

**Pool exhaustion looks the same.** When every pooled connection is busy,
acquisition waits at most `CLOUDSCALE_PG_POOL_TIMEOUT_SECONDS` (default 3)
and then fails as a transient error → breaker → 503. If 503s coincide with
a healthy database and high `cloudscale_http_request_seconds`, raise
`CLOUDSCALE_PG_POOL_MAX` (default 4) or add replicas; do not raise the
timeout — that trades fast failure for slow failure.

---

## R5 — 429s: rate limiting

**Signals.** `status="429"` counters rising for a subject.

**Check.** Is it one subject (abuse or a misbehaving client) or everyone
(limit too low)? Access logs carry `subject`. Adjust
`CLOUDSCALE_RATE_LIMIT_PER_MINUTE`. On the PG tier with
`CLOUDSCALE_RATE_LIMIT_BACKEND=postgres` the budget is shared across replicas;
with `memory` each replica has its own budget (limit × replicas).

---

## R6 — Identity: key rotation, compromised admin token

**Rotation (JWKS mode).** Publish the new key under a new `kid` at the JWKS
URL, start signing with it, keep the old key published until old tokens
expire (≤ `CLOUDSCALE_JWT_MAX_LIFETIME_SECONDS`, default 1 h), then remove
it. No restart: an unknown `kid` triggers a refetch.

**Rotation (shared-secret mode).** Requires a restart with the new
`CLOUDSCALE_JWT_SECRET`; tokens signed with the old secret are rejected
immediately. Coordinate with token issuers.

**Compromised admin token.** Add its `jti` to `CLOUDSCALE_JWT_REVOKED_JTIS`
and restart. Admin tokens without a `jti` are already refused. Non-admin
tokens are bounded by the 1 h lifetime cap; rotate keys if the issuer is
compromised.

---

## R7 — Backup and restore (PostgreSQL)

The system of record is the `events` table plus `command_results`
(idempotency) and `event_envelopes` (trace identity). Everything else is
derived:

- `balances`, `processed_events`, `consumer_offset` — rebuildable by
  truncating them, resetting `consumer_offset.last_id = 0`, and letting the
  consumer replay (`processed_events` guarantees exactly-once on replay).
- `outbox` + `events.published` — rebuildable: truncate `outbox`, set
  `published = false`, the relay republishes in id order.
- `dead_letters`, `accounts`, `rate_limit_buckets` — small; include in backups.

Use `pg_dump` of the whole database at a consistent snapshot; restore with
`pg_restore`, then `migrate current` to confirm the revision matches the
build before starting processes in `migrations` mode.

---

## R8 — Consumer leader failover (what normal looks like)

When a leader process dies, PostgreSQL releases its session-level advisory
lock; a standby's next `try_acquire` (poll interval, default 20 ms) succeeds.
Expected trace: standby logs `consumer.lease.acquired`, its
`cloudscale_consumer_is_leader` goes to 1, lag drains. Verified by a
two-process kill test during development (`SIGKILL` on the leader; standby
drained five backlog events within one second).
