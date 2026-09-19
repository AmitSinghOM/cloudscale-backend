# Configuration reference

Every setting is an environment variable prefixed `CLOUDSCALE_`. Nothing is
read from files. This page is checked by
`tests/architecture/test_configuration_docs.py`: a variable referenced in
code but missing here fails the build, so the table cannot drift.

Defaults are the safe choice for a laptop; the **Production** column is what
a customer-facing deployment must set (RUNBOOK D1–D7).

## Storage

| Variable | Default | Production | Meaning |
|---|---|---|---|
| `CLOUDSCALE_STORAGE` | `sqlite` | `postgres` | Storage tier. `sqlite` is single-host and in-process; `postgres` is pooled and multi-replica. |
| `CLOUDSCALE_LOG_DB` | — (required for sqlite) | n/a | Path of the SQLite event-log file (also holds idempotency records and the ownership registry). |
| `CLOUDSCALE_PROJECTION_DB` | — (required for sqlite) | n/a | Path of the SQLite projection file (balances, offsets, dead letters). |
| `CLOUDSCALE_PG_DSN` | — (required for postgres) | from secrets manager | libpq connection string. |
| `CLOUDSCALE_PG_SCHEMA` | `auto` | `migrations` | `auto`: the app creates tables idempotently (dev/test). `migrations`: the app creates **nothing** and refuses to start unless Alembic has stamped the required revision. |
| `CLOUDSCALE_PG_POOL_MAX` | `4` | size for your replica count | Max pooled connections per process. Raise this (or add replicas) when 503s coincide with a healthy database. |
| `CLOUDSCALE_PG_POOL_TIMEOUT_SECONDS` | `3` | `3` | Wait for a pooled connection before failing fast as a transient error → 503 + `Retry-After`. Do not raise it to hide exhaustion. |
| `CLOUDSCALE_SNAPSHOT_EVERY` | `100` | `100` | ADR-0012: write a verified stream snapshot every N events on a stream (both tiers), keeping command latency flat against stream depth. `0` disables writing; existing snapshots are still read and verified against the log. Drop snapshots at any time with `scripts/snapshots.py`. |

## Identity (exactly one of the first two is required)

| Variable | Default | Production | Meaning |
|---|---|---|---|
| `CLOUDSCALE_JWT_SECRET` | — | avoid; prefer JWKS | HS256 shared secret, ≥ 32 bytes. Rotation requires a restart. |
| `CLOUDSCALE_JWT_JWKS_URL` | — | your IdP's JWKS URL | OIDC/JWKS mode (RS/ES families). Unknown `kid` triggers a bounded (3 s) refetch. |
| `CLOUDSCALE_JWT_ALGORITHMS` | family of the mode | as issued | Allowed algorithms; must match the mode's family (blocks `alg` confusion). |
| `CLOUDSCALE_JWT_ISSUER` | `cloudscale` | your issuer | Required `iss` claim value. |
| `CLOUDSCALE_JWT_AUDIENCE` | unset | your audience | If set, `aud` is required and must match. |
| `CLOUDSCALE_JWT_MAX_LIFETIME_SECONDS` | `3600` | `≤ 3600` | Tokens whose `exp − iat` exceeds this are rejected; bounds a leaked token's life. |
| `CLOUDSCALE_JWT_REVOKED_JTIS` | empty | as needed | Admin-token `jti` denylist (JSON list). Admin tokens without `jti` are refused. Changing it requires a restart. |

## Authorization and abuse controls

| Variable | Default | Production | Meaning |
|---|---|---|---|
| `CLOUDSCALE_QUERY_AUTH_REQUIRED` | `true` | `true` | Reads require a token. Only relax for a public read-only demo. |
| `CLOUDSCALE_RATE_LIMIT_PER_MINUTE` | `600` | tuned | Per-subject budget after authentication. `0` disables (bench only). |
| `CLOUDSCALE_RATE_LIMIT_BACKEND` | `memory` | `postgres` | `memory` is per replica; `postgres` shares one budget across replicas. |
| `CLOUDSCALE_CLIENT_RATE_LIMIT_PER_MINUTE` | `1200` | tuned | Per-client-address budget **before** authentication; bounds the cost of unauthenticated floods. Per replica. `0` disables. |
| `CLOUDSCALE_TRUST_PROXY_HEADERS` | `false` | `true` only behind a controlled proxy | Take the client address from `X-Forwarded-For`. Enabling it without a proxy that overwrites the header lets clients spoof out of the client limiter. |
| `CLOUDSCALE_MAX_BODY_BYTES` | `16384` | `16384` | Request body cap; enforced before parsing. |
| `CLOUDSCALE_CORS_ORIGINS` | empty (closed) | your origins | Browser origins allowed (JSON list). |

## Consumer process

| Variable | Default | Production | Meaning |
|---|---|---|---|
| `CLOUDSCALE_CONSUMER_NAME` | `balances` | `balances` | Lease name: exactly one leader per name across all replicas. |
| `CLOUDSCALE_CONSUMER_METRICS_PORT` | unset (no server) | set, network-restricted | Prometheus endpoint for `is_leader`, `lag_events`, `last_drain_timestamp`, counters. |
| `CLOUDSCALE_CONSUMER_POLL_SECONDS` | `0.02` | `0.02` | Standby retry interval for the lease and idle sleep for the leader; bounds failover time. |

## Test-only

| Variable | Meaning |
|---|---|
| `CLOUDSCALE_TEST_PG` | Admin DSN for PostgreSQL-gated tests (they create and drop throwaway databases). CI sets it and fails if those tests skip. |

## Not configuration

Retention thresholds are CLI flags of `python -m cloudscale.entrypoints.retention`
(see RUNBOOK R9); the Alembic URL is taken from `CLOUDSCALE_PG_DSN`.
