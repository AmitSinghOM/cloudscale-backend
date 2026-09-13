# Security Policy

## Reporting a vulnerability

Please **do not** open a public issue for security reports.

Use GitHub's private vulnerability reporting on this repository
(*Security → Report a vulnerability*). If that is unavailable, contact the
maintainer via the profile linked on the repository. You will receive an
acknowledgement within 3 business days and a triage decision within 10.

Include: affected version or commit, deployment tier (SQLite / PostgreSQL),
reproduction steps, and impact as you understand it. Coordinated disclosure
is welcome; a fix and advisory will be published together.

## Supported versions

| Version | Supported |
|---|---|
| `main` and the latest tagged release | yes |
| older tags | no — upgrade |

## Scope

In scope: anything reachable through the HTTP API, the consumer process, the
migration entrypoint, the container image, or the documented configuration.

Out of scope: the hosting environment (TLS termination, network policy,
secret storage) — these are stated deployment assumptions in
`docs/RUNBOOK.md`; vulnerabilities that require an attacker to already hold
a valid admin token *and* database credentials; volumetric attacks against
the deployment's network edge.

## What is enforced today

Verified by tests in this repository and by CI on every pull request:

- **Authentication.** JWT (HS256 with a ≥32-byte secret) or OIDC/JWKS
  (RS/ES families) with `kid` rotation; algorithm family is validated so an
  `alg` downgrade is rejected; `iss`, `sub`, `iat`, `exp` required; lifetime
  capped at `CLOUDSCALE_JWT_MAX_LIFETIME_SECONDS` (default 1 h); admin tokens
  require `jti` and are revocable via `CLOUDSCALE_JWT_REVOKED_JTIS`.
- **Authorization.** Default deny per account. Access requires the admin
  scope, an `accounts` claim naming the account, or ownership registered via
  `POST /v1/accounts`. Reads may be relaxed only by explicit setting.
- **Abuse controls.** Pre-authentication per-client budget (bounds token
  verification cost), request body cap, per-subject budget (shared across
  replicas on the PostgreSQL tier), CORS closed by default.
- **Data integrity.** Idempotent command unit of work — a retried
  `command_id` can never double-apply; append-only event log; audit log of
  every command outcome and registration.
- **Schema safety.** Production mode refuses to start against a database
  Alembic has not stamped at the required revision.
- **Supply chain.** Exact-pinned dependencies with hash-verified installs;
  non-root, multi-stage image; `pip-audit` on the production lock and a
  Trivy image scan fail the build on fixable CRITICAL/HIGH findings;
  Dependabot for Python, Actions, and the base image.

The threat model, including accepted risks, is in `docs/THREAT_MODEL.md`.

## Known limitations (accepted, documented)

- Admin `jti` revocation requires a restart (environment-sourced list).
- The audit log is structured stdout; tamper evidence depends on the log
  pipeline the deployment ships it to.
- `/metrics` is unauthenticated by design and must be network-restricted.
- No independent penetration test has been performed yet.
