# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions
are git tags on `main`. Entries say what changed for an operator or
integrator; the commit history says how.

## [Unreleased]

Developer experience: from clone to a correct first integration without
reading source.

### Added
- `make dev` / `make token` / `make stop`: API + consumer on the SQLite tier
  with a fixed local-only secret; `make token ARGS=--curl` prints a
  paste-ready deposit. `make dev` refuses to start if the port is taken.
- `compose.yaml`: production-shaped stack (PostgreSQL 17 → Alembic migrate →
  API + consumer, `migrations` mode). CI brings it up and asserts
  `/v1/ready`, an accepted deposit, and the balance read model.
- `docs/CONFIGURATION.md`: every `CLOUDSCALE_*` variable with default and
  production value; a test fails the build on undocumented or stale entries.
- `docs/API_ERRORS.md`: every status and error code with the correct client
  action; a test fails the build if a domain code or outcome is missing.
- `docs/openapi.json`: the committed HTTP contract; a test fails the build
  when the running app's schema drifts from it.
- `examples/python_client.py`: copy-paste client showing same-`command_id`
  retry, 409 handling, and read-your-write polling; exercised against a real
  server by `tests/smoke/`.
- `make help`; `.devcontainer/` for Codespaces / VS Code.

## [0.6.0] — 2026-09-13

Longevity: the structure and enforcement that keep the service safe to
change through 2033.

### Added
- Event schema evolution (ADR-0009): `cloudscale.domain.upcasting` registry,
  `schema_version` on every `events` row (Alembic `0003_events_schema_version`;
  SQLite files upgrade in place), upcasting at both read boundaries, fixture
  corpus under `tests/fixtures/events/`. Events from a newer build dead-letter
  instead of halting the consumer.
- CI runs the gate on Python 3.12 and 3.13 (ADR-0010); `requires-python`
  widened to `<3.14`.
- Lock-freshness check `scripts/check_lock_age.sh` in CI: fails when a lock
  file is more than 120 days since regeneration (warns at 90).
- Soak harness `scripts/soak_run.py` with fixed pass criteria (PR #15).
- Longevity structure: `docs/LONGEVITY.md`, `docs/adr/` (ADR-0001..0010),
  `CONTRIBUTING.md`, `AGENTS.md`, `CODEOWNERS`, this changelog.

### Fixed
- Retention job crashed on deployments without the PostgreSQL rate-limiter
  backend (`rate_limit_buckets` absent). Missing tables are now "nothing
  to prune" (PR #15).

## [0.5.1] — 2026-09-13

Production-readiness hardening from the CTO / staff-security review.

### Added
- `GET /v1/ready` readiness probe: storage round-trip plus Alembic revision
  check in `migrations` mode; 503 on failure. `/v1/health` is liveness only.
- Pre-authentication per-client rate limit (`CLOUDSCALE_CLIENT_RATE_LIMIT_PER_MINUTE`,
  `CLOUDSCALE_TRUST_PROXY_HEADERS`).
- CI: `pip-audit` on the production lock, Trivy image scan, Dependabot.
- `SECURITY.md`, `docs/THREAT_MODEL.md`.
- Retention job `python -m cloudscale.entrypoints.retention`; Alembic
  `0002_command_results_created_at`.
- RUNBOOK deployment assumptions D1–D7 and retention procedure R9.
- `ruff` S (bandit) rules in the gate.

### Changed
- Pool acquisition wait bounded by `CLOUDSCALE_PG_POOL_TIMEOUT_SECONDS`
  (default 3 s, was 30 s); JWKS fetch bounded at 3 s (was 30 s).
- `confluent-kafka` moved to the optional `[kafka]` extra.
- Runtime image applies Debian security updates at build time.
- Docker `HEALTHCHECK` targets `/v1/ready`.

### Fixed
- `/v1/ready` no longer echoes driver error text.
- Three runtime `assert`s used as guards replaced with explicit errors.

## [0.5.0] — 2026-09-12

Phase 5: production readiness. Claims- and ownership-based authorization,
structured/audit logging, Prometheus metrics, abuse controls, Docker + CI,
PostgreSQL pooling and transactional outbox, OIDC/JWKS identity with
lifetime cap and admin `jti` revocation, Alembic migrations with fail-closed
production mode, shared PostgreSQL rate limiter, consumer HA via session
advisory lock, runbook and SLOs.

## [0.4.0] — 2026-09

Phase 4: HTTP tier with real-process gate harness and per-commit evidence.

## [0.3.0] — 2026-09

Phase 3: load profiling; O(n) withdraw-guard replay replaced by a memoized
fold (29.6× hot-path throughput).

## [0.2.0] — 2026-08

Phases 0–2: hexagonal core, idempotent command unit of work, resilient
consumer with retry, circuit breaker and dead-letter redrive.

[Unreleased]: https://github.com/AmitSinghOM/cloudscale-backend/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/AmitSinghOM/cloudscale-backend/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/AmitSinghOM/cloudscale-backend/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/AmitSinghOM/cloudscale-backend/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/AmitSinghOM/cloudscale-backend/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/AmitSinghOM/cloudscale-backend/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/AmitSinghOM/cloudscale-backend/releases/tag/v0.2.0
