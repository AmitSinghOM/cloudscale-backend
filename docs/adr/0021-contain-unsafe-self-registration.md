# ADR-0021: Contain unsafe self-registration

**Status:** Accepted
**Date:** 2026-10-03
**Enforced by:** `tests/unit/entrypoints/test_account_registry.py::test_self_registration_denied_without_binding_or_event`, `test_existing_registry_owner_access_survives_self_registration_denial`; `tests/unit/entrypoints/test_restored_replay.py::test_self_registration_cannot_claim_existing_financial_stream` (SQLite and PostgreSQL); `tests/architecture/test_openapi_contract.py`.

## Context

The old `POST /v1/accounts` allowed any authenticated subject to bind an account ID on a first-claim basis, even when a different principal had already written financial events to its unregistered stream. Registry uniqueness prevents only later claims, not the initial unauthorized binding. ADR-0017 proposes event storage without resolving allocation authority; ADR-0019 proposes full trusted provisioning but its allocation source, migration, issuer and cross-writer guard are not yet approved.

## Decision

The old self-registration HTTP operation stays routable for authenticated clients but unconditionally returns the existing generic account-authorization 403 for a well-formed request. It never looks up account existence, writes a binding, event or result, or reports an ownership-dependent outcome; claims and administrator scope are not exceptions. Standard authentication and request-shape validation remain in place: a missing/invalid bearer token returns 401 for a well-formed body and a malformed body may return 422 before authorization. Keep registry ownership reads and pre-existing bindings: payments, authorized claims/admin operations and all-debit checks retain their current semantics. Do not introduce a provisioning route or any database migration in this containment change.

## Alternatives rejected

- Preserve first-claim registration for empty or administrator-supplied IDs: neither an empty stream nor admin scope proves beneficiary allocation, and existing funded streams are claimable.
- Remove the ownership registry or disable payments: breaks legitimate account access and spending without closing other issuer-claim paths.
- Return 501 when no registry is configured or 409 for taken IDs: unnecessarily exposes configuration or ownership state; use one 403 regardless of account state.

## Consequences

New account onboarding through this endpoint stops. Operators must inventory callers, agree a production cutover and examine existing bindings and claim/admin grants before deployment; no deployment, ownership backfill or rollback that reopens first-claim registration is authorized. The access/request log records denials; no `account.registered` success record is emitted. ADR-0019 remains Proposed for a future independently reviewed allocation authority, new route, serialization and legacy reconciliation. ADR-0017 remains Proposed and its promise of an unchanged self-registration HTTP contract is incompatible with this accepted decision; it must be superseded or revised before implementation.
