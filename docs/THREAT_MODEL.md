# Threat Model — CloudScale Backend

Scope: v0.5.x, PostgreSQL tier as deployed per `docs/RUNBOOK.md`. Method:
STRIDE per trust boundary. Every mitigation cites the code or test that
enforces it; anything without a citation is an accepted risk and is listed
as such. Review this document whenever a boundary changes.

## Assets

| Asset | Why it matters |
|---|---|
| Account balances (projection) and the event log (source of record) | Financial integrity; the reason the service exists |
| Command idempotency records | Break these and retries double-apply |
| Signing secret / JWKS trust | Forge tokens → full compromise |
| Ownership registry | Controls who may act on which account |
| Audit log | Forensics and non-repudiation |
| Availability of the command path | Customers cannot transact |

## Trust boundaries and actors

```
 Internet ──TLS──▶ [Edge proxy] ──▶ [HTTP tier ×N] ──▶ [PostgreSQL]
                                        │                   ▲
                    [Identity provider]◀┘ JWKS         [Consumer ×N, 1 leader]
                                                            │
                                            [Metrics scraper] [Log pipeline]
```

Actors: anonymous internet client; authenticated end user; authenticated
admin (`accounts:admin`); operator with shell/DB access; identity provider;
upstream dependency authors.

B1 Internet → edge. B2 Edge → HTTP tier. B3 HTTP tier → PostgreSQL.
B4 Identity provider → HTTP tier (JWKS fetch). B5 Consumer → PostgreSQL.
B6 Operators → everything. B7 Supply chain → image.

## Threats and mitigations

### B1/B2 — Requests from the network

| STRIDE | Threat | Mitigation | Evidence |
|---|---|---|---|
| S | Forged or replayed bearer token | Signature verified against secret or JWKS; `alg` family pinned; `iss` checked; `exp` enforced; `iat` required and lifetime ≤ 1 h so a leaked token has bounded life | `entrypoints/http/verifiers.py`, `auth.py`; `tests/unit/entrypoints/test_http_app.py`, `test_oidc_jwks.py` |
| S | `alg: none` / RS→HS confusion | Allowed algorithms are an explicit family set validated at settings load; PyJWT given the list, never the token's own `alg` | `settings.py` alg-family validator; verifier tests |
| S | `kid` pointing at attacker-controlled key | JWKS URL is operator-configured; unknown `kid` triggers a refetch from that URL only | `JwksVerifier` |
| T | Body tampering / oversized payloads | Pydantic models reject unknown shapes; body cap 16 KiB before parsing | `limits.BodySizeLimitMiddleware`; 413 test |
| R | Caller denies issuing a command | Audit log records subject, account, `command_id`, outcome | `observability.audit_command`; audit tests |
| I | Auth failure oracle (user vs. password vs. scope) | Single fixed 401 message for every authentication failure | `auth._unauthorized`; test asserting identical bodies |
| I | Cross-account read | Default deny; grant only via admin scope, `accounts` claim, or registry ownership | `auth.authorize_account`; ownership tests incl. cross-connection race |
| D | Unauthenticated flood exhausting CPU on token verification | Pre-auth per-client budget before any verification; probes exempt | `limits.ClientRateLimitMiddleware`; flood test `401,401,401,429,429` |
| D | Authenticated subject floods | Per-subject budget; shared across replicas via PostgreSQL backend | `RateLimiter`, `PostgresRateLimiter`; 429 tests |
| D | Spoofed `X-Forwarded-For` to escape client budget | Header ignored unless `trust_proxy_headers`; documented as edge-only | test `forwarded_for_ignored_unless_proxy_trusted` |
| E | Non-admin registers ownership of someone else's account | First registration wins; 409 on conflict; admin bypass requires `jti` | registry tests |
| E | Revoked admin token still used | `jti` denylist checked on every admin request | revocation tests |

### B3/B5 — Service → PostgreSQL

| STRIDE | Threat | Mitigation | Evidence |
|---|---|---|---|
| T | Retried command applied twice | Idempotency record written in the same transaction as the events; duplicate `command_id` returns stored result | `application/command_execution.py`; UoW contract tests both tiers |
| T | Partial write on crash mid-command | Single transaction per command; PostgreSQL autocommit trap fixed (was silently committing per statement) | savepoint/visibility regression test |
| T | Event replayed to projection twice | `processed_events` claim per event id; redrive keeps the claim | resilient-consumer and redrive tests |
| T | Deploy runs against wrong schema | `CLOUDSCALE_PG_SCHEMA=migrations` refuses to start unless Alembic revision matches; readiness re-checks it | `pool.verify_migrated`; migrations tests; `/v1/ready` 503 test |
| D | Two consumers race on the projection | Session advisory lock: one leader per consumer name; lock dies with the connection | `consumer_lease.py`; exclusivity + failover tests; two-process SIGKILL run |
| D | Database outage | Command path circuit breaker + retry → 503 with `Retry-After`; consumer breaker halts without advancing offset; readiness goes 503 | breaker tests; live ready 200→503 |
| I | SQL injection | All queries parameterised via psycopg; no string-built SQL from request data | code review; ruff `S` rules not enabled — **see accepted risks** |

### B4 — Identity provider

| STRIDE | Threat | Mitigation | Evidence |
|---|---|---|---|
| S | Compromised IdP signing key | Rotation procedure (R6); lifetime cap bounds blast radius to ≤ 1 h of tokens | `docs/RUNBOOK.md#r6` |
| D | JWKS endpoint down | PyJWKClient caches keys; existing `kid`s keep verifying; new `kid`s fail closed | verifier behaviour (library) |

### B6 — Operators

| STRIDE | Threat | Mitigation | Evidence |
|---|---|---|---|
| T | Direct DB edits to balances | Balances are derived; rebuild from the event log detects drift (R7) | runbook procedure — **not automated** |
| R | Operator disputes an action | Audit log; DB-level actions are outside the application's audit — **accepted** | — |
| I | Secrets in environment visible to co-tenants | Documented assumption: secrets injected by a manager into the process env only | `docs/RUNBOOK.md` deployment assumptions |

### B7 — Supply chain

| STRIDE | Threat | Mitigation | Evidence |
|---|---|---|---|
| T | Malicious or vulnerable dependency | Exact pins + hash-verified install; `pip-audit` on the production lock fails CI; Trivy scans the image for OS and library CVEs; Dependabot weekly | `.github/workflows/ci.yml`, `.github/dependabot.yml`, `requirements.lock` |
| T | Unused native code in the image | Kafka client removed from the default install (optional `[kafka]` extra) | `pyproject.toml` |
| E | Container escape via root | Non-root user, multi-stage build, no build tools in the runtime layer | `Dockerfile` |

## Accepted risks (dated; revisit at each release)

1. **Admin revocation needs a restart.** The `jti` denylist is environment
   sourced. Acceptable at pilot scale; a live denylist (DB or cache) is the
   GA fix. *2026-09-13*
2. **Audit log tamper evidence is delegated.** Structured stdout; the
   deployment must ship it to an append-only sink. *2026-09-13*
3. **`/metrics` unauthenticated.** Exposes route names and traffic shape;
   must be network-restricted. *2026-09-13*
4. **No static security lint (`ruff` `S` rules / Bandit).** Parameterised SQL
   is enforced by review, not tooling. Low cost to add; scheduled for the
   next hardening PR. *2026-09-13*
5. **No independent penetration test.** Required before the first external
   contract. *2026-09-13*
6. **Unbounded table growth** (`command_results`, `dead_letters`,
   `rate_limit_buckets`, event log without snapshots). Retention job in
   progress; until then a documented ceiling. *2026-09-13*

## Out of scope

Physical security, the hosting provider's hypervisor, and the identity
provider's internal security. Denial of service at the network edge
(volumetric) is the edge proxy's responsibility.
