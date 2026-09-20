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
| Hold sweeper | `python scripts/sweep_holds.py --dsn …` | scheduled (cron/Job), any number concurrently | exit code; prints `expired=N skipped=M` |

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
| D8 | **Scheduled `pg_dump` of the whole database, and WAL archiving if the RPO must be seconds rather than the dump interval.** The system of record is four tables (R7); the deployment chooses how often they are captured. | The service has one database and no replication story of its own (ADR-0016); RPO is a deployment property. | The latest dump restores through `scripts/restore_drill.py --dump` with exit 0; `docs/SLO.md` § Recovery states the interval. |

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

**Rolling back a deploy after a schema-version bump.** Events written by a
newer build carry a higher `schema_version`. An older build cannot translate
them: the consumer dead-letters each one with
`error_type = UnknownSchemaVersionError` (R3) rather than halting, and the
command path returns 503 for accounts whose stream contains one. Balances
for those accounts stay frozen until the newer build is redeployed — then
`scripts/dlq.py … redrive --all` applies the parked events. Nothing is lost.

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

Every table has one class, declared in code
(`cloudscale/adapters/postgres/schema.py`, ADR-0016). This list is tested
against that code and against the migrated schema; if you are reading a
copy that disagrees with `schema.py`, the code wins.

**System of record** — lost data is lost money, idempotency or ownership.
The backup is these:
- `events` — the log (ADR-0002). `events.published` is a derived column, see below.
- `event_envelopes` — trace identity for every event.
- `command_results` — idempotency. A persisted rejection has no event, and a
  persisted acceptance is what makes a client's retry a replay instead of a
  second deposit; a restore without it turns every in-flight retry into a
  duplicate command.
- `accounts` — ownership registrations are written outside the log (see
  ADR-0016 alternatives for the plan to make them events).

**Derived** — rebuilt from `events`; a restore may truncate every one of these:
- `balances`, `holds`, `transfers`, `transfer_legs`, `processed_events`,
  `consumer_offset`, `dead_letters` — the consumer rebuilds them: truncate,
  and the consumer (which re-creates its offset row at 0) replays the whole
  log; `processed_events` guarantees exactly-once on replay; a poison event
  dead-letters again.
- `outbox` (with `events.published`) — truncate `outbox`, set
  `published = false`, and the relay republishes in id order.
- `stream_snapshots` — a cache (ADR-0012); the next fold rewrites it. A
  restore from a different dump is detected per stream (anchor mismatch is
  logged at WARNING and the stream is refolded from the log).

**Ephemeral** — neither backed up nor rebuilt:
- `rate_limit_buckets` — a restart forgives a budget.

**Procedure.** `pg_dump --format=custom` the whole database (a consistent
snapshot; carrying the derived tables makes a straight restore fast).
Restore with `pg_restore` into a fresh database, run
`python -m cloudscale.entrypoints.migrate current` and confirm it prints the
revision this build requires (`schema.CURRENT_REVISION`), then start
processes in `migrations` mode. If any read model is suspect after the
restore, rebuild it per the Derived list rather than trusting it.

**The drill.** `scripts/restore_drill.py` executes this procedure end to end
and refuses to pass unless the rebuilt read models equal both a full fold of
the restored log and the read models carried in the dump:

    python scripts/restore_drill.py --dsn postgresql://…/postgres --seeded     # CI, every release
    python scripts/restore_drill.py --dsn postgresql://…/postgres --dump FILE  # annual, on a production dump

`--dsn` is an administrative connection able to `CREATE DATABASE`; every
database the drill creates is named `cloudscale_drill_*` and dropped unless
`--keep`. `pg_dump`/`pg_restore` must be the server's major version
(`--pg-bin DIR` or `CLOUDSCALE_PG_BIN` points at them; a mismatch exits 2).
Exit 0 pass, 3 a comparison failed (the report names which), 2 tooling.
The report (`--report PATH`, default under `evidence/<sha>/restore-drill/`)
records dump, restore, verify and rebuild durations — the RTO components —
and `rpo_events`, the log tail at report time minus the tail in the dump,
which a production drill must read honestly. See `docs/SLO.md` § Recovery.
SQLite: `--sqlite-dir DIR --seeded` runs the same steps over a log file and a
projection file (the backup API stands in for `pg_dump`).

---

## R10 — A balance disagrees with a full replay

Symptom: a `GET .../balance` or a command decision disagrees with what a
hand fold of `events` for that stream says.

1. Drop the stream's snapshot and re-read:
   `python scripts/snapshots.py <log-path-or-dsn> drop <account_id>`.
   The next fold is a full fold from `seq = 1` and writes a fresh snapshot.
2. If the disagreement persists, the snapshot was not the cause. Look at
   the log itself (`events` ordering, a stuck consumer per R2, a dead
   letter per R3) — do not edit `stream_snapshots` by hand, and never edit
   `events`.
3. `grep snapshot.rejected` in the API log tells you *why* a snapshot was
   discarded (state-version change, anchor mismatch after a restore,
   truncation). A steady stream of rejections for one account means every
   command on it is paying a full fold; `stats` shows whether snapshots are
   being written at all (`CLOUDSCALE_SNAPSHOT_EVERY=0` disables writing).

---

## R8 — Consumer leader failover (what normal looks like)

When a leader process dies, PostgreSQL releases its session-level advisory
lock; a standby's next `try_acquire` (poll interval, default 20 ms) succeeds.
Expected trace: standby logs `consumer.lease.acquired`, its
`cloudscale_consumer_is_leader` goes to 1, lag drains. Verified by a
two-process kill test during development (`SIGKILL` on the leader; standby
drained five backlog events within one second).

---

## R11 — Holds: expiry is not happening, or `held` looks wrong

Holds (ADR-0014) reserve funds; a hold past `expires_at` is released only when
`scripts/sweep_holds.py` runs. Nothing in the fold reads a clock.

1. **Holds never expire.** The sweeper is not scheduled or its DSN is wrong.
   Run it by hand: `python scripts/sweep_holds.py --dsn <dsn>` (SQLite:
   `--log <log> --projection <projection>`). Exit 2 means it could not open a
   migrated database. `expired=0 skipped=0` with open holds visibly past
   expiry means the consumer is behind (R2): the sweeper reads the `holds`
   read model.
2. **`skipped` is high.** Normal under concurrency: a skip is a hold that
   another sweeper, a post or a void resolved first, or a stream whose
   version moved between the sweeper's read and its command. The log is the
   truth: `SELECT COUNT(*) FROM events WHERE type = 'HoldReleased' AND
   release_reason = 'expired' AND transfer_id = '<hold_id>'` is 0 or 1, never
   more, because the sweeper's command id is deterministic per hold.
3. **`held` disagrees with the open holds.** `held` is folded from the
   source stream's `Hold*` events with `HELD_SIGN`; the `holds` table is a
   projection of the same events. Rebuild the projection (R7 rebuild
   procedure) before suspecting the log; then compare `held` against
   `SELECT SUM(amount) FROM holds WHERE source = ? AND state = 'open'`. If the
   two agree and a client disagrees, the client is reading `balance` where it
   should read `available`.
4. **A post is refused with `hold_expired` but the sweeper has not released
   it.** Correct: post checks the decision clock, release waits for the
   sweeper. `available` is restored on the next sweep; void it manually if
   the funds are needed sooner.
