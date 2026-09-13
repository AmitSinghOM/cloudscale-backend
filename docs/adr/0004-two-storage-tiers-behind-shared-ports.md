# ADR-0004: Two storage tiers behind shared ports

**Status:** Accepted  **Date:** 2026-08 (reconstructed 2026-09-13)
**Enforced by:** contract tests run against both adapters; PG-gated tests
asserted non-skipped in CI

## Context
Local development and CI need zero infrastructure; production needs
concurrency, durability and horizontal scale.

## Decision
SQLite (single-host, in-process) and PostgreSQL (pooled, multi-replica)
implement the same ports. Contract tests run against both. The SQLite tier
is a real tier, not a mock: it has its own gate evidence.

## Alternatives rejected
- *PostgreSQL only (Docker in dev).* No Docker on the original development
  host; slower feedback; CI still needed a real PG service.
- *In-memory fakes for tests.* Tests would prove the fake, not the store.

## Consequences
Two adapters to keep in step; the migrations test asserts the auto-created
and Alembic-created PG schemas are identical. Performance claims must state
the tier (the tiers have different ceilings: ROADMAP v0.5.0 note).
