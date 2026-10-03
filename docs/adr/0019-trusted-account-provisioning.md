# ADR-0019: Trusted account provisioning

**Status:** Proposed
**Date:** 2026-10-03
**Enforced by:** Full trusted provisioning is not implemented. The separately
approved containment is recorded by ADR-0021; the decisions and regression
matrix below remain prerequisites for the new provisioning operation.

## Context

Before ADR-0021, `routes/accounts.py::register_account` bound a supplied
account ID to the caller subject. Registry uniqueness stopped a second owner,
not an incorrect first owner. Claims-authorized commands and credit legs can
still create financial streams before registration. An empty balance is not
proof of an empty stream or ownership. The all-debit fix (ADR-0018) alone did
not close the gap; ADR-0021 separately disables the old self-registration path.

ADR-0017 proposes event-backed ownership storage, not permission to assert
ownership. It remains Proposed and must be reconciled with this policy before
implementation; its unchanged self-registration HTTP contract is incompatible
with accepted containment ADR-0021. Other accepted ADRs are not superseded by
this draft.

## Decision proposed for approval

Only a dedicated onboarding workload may bind an allocated account to its
beneficiary. The workload verifies an independently maintained allocation
record before calling the ledger. The beneficiary never becomes the privileged
caller merely by signing in. Neither a user-chosen account ID, an `accounts`
claim, an empty stream, nor administrator scope proves allocation authority.

The real allocation system, accountable operator, and beneficiary identity
lifecycle have **not been identified/confirmed**. Do not invent an upstream
service or ship a permissive fallback. Without that authority, provisioning
remains disabled, including for empty streams.

### Containment and cutover

The first security change is independently approved for local implementation
(ADR-0021): retain existing ownership reads and command access, but deny
authenticated calls to the old self-registration operation with generic 403
and no writes. Do not disable the registry dependency itself: existing owners
still need `owner_of` for authorization. Anonymous callers keep 401 for
otherwise valid requests. Production cutover/deployment remains separately gated.

Propose a new `POST /v1/provisioned-accounts` operation for the designated
workload. Keep the old path denied, not as a compatibility bypass. Client
inventory and cutover approval are prerequisites; no automatic production
rollout or re-enablement of unsafe self-registration is a rollback strategy.
The safe rollback is to disable new provisioning while preserving bindings.

### Authentication and identity

- Require a dedicated `accounts:provision` permission and an allowlisted
  issuer/subject pair, a dedicated audience, a required `jti`, valid signature,
  and a positive lifetime capped at five minutes. Proposed lifetime must be
  confirmed with the issuer owner. Default deny missing/malformed credentials.
- Production provisioning uses trusted asymmetric issuer verification, not
  shared development secrets. Reuse the existing verifier interface, but a
  dedicated audience needs route-specific verification: do not widen the
  ordinary account API's audience acceptance to make provisioning work.
- `accounts:admin`, ordinary account claims, and registry ownership do not
  imply provisioning permission. Provisioning does not grant the machine
  spending access to the beneficiary's account.
- The beneficiary identity must belong to the configured account-holder
  issuer/namespace. The current registry stores a bare subject and the API
  trusts one issuer; an issuer change is a reviewed identity migration, never
  an assumption that matching `sub` strings name the same person.
- Require a documented, tested revocation bound across replicas. The existing
  environment-sourced denylist requires coordinated process replacement;
  five-minute expiry bounds token lifetime but does not itself revoke tokens.
  If that operational bound is unacceptable, approve a shared revocation
  mechanism separately. Do not claim immediate revocation without evidence.

### Request, replay, and persistence

Proposed request fields: `account_id`, `owner_subject`, and `provisioning_id`.
The service identity comes from the verified token, not the request body.
These field names describe a proposed contract, not an available API.

Persist the account, beneficiary namespace/subject, allocation ID, authenticated
service identity, and outcome atomically. A durable allocation identity binds
these fields and cannot expire with the ordinary seven-day command-result
window. New binding returns 201; an exact authorized allocation replay returns
200; changed account/beneficiary or conflicting legacy registration returns
409 without exposing another owner's identity or financial state. Losing
provisioning authorization prevents replay before stored-outcome lookup.

Do not derive durable operation identity from `jti`: normal credential renewal
must not create another allocation. A different workload taking over an
allocation requires an approved handover policy, not silent owner replacement.
New provenance/outcome storage needs a migration and system-of-record
classification. This ADR deliberately does not move ownership into events or
reclassify `accounts` as derived; ADR-0017 is independent follow-on work.

Malformed bodies may receive 422 before authentication/authorization because
of framework validation order. Accept that request-shape disclosure in this
proposal; never read account existence before privilege checks. Well-formed
non-provisioner requests receive the same generic 403 for all account states.
Denied or invalid requests write no binding, event, or result.

### Stream lifecycle and races

Only freshly allocated IDs with **no financial history** are eligible for the
new provisioning path. A zero balance after earlier activity is ineligible.
Reject legacy registrations without allocation provenance even if the subject
matches. Existing funded/unregistered streams require separate, approved
reconciliation; never assign ownership from a caller claim or fabricated
backfill. Quarantining registration is not a freeze of existing claims/admin
command access; those grants need review during rollout too.

The empty-stream check and binding must be serialized against **all** financial
writers on both tiers. PostgreSQL's registry primary key and event-stream
sequence constraints do not conflict with each other today. A standalone
`SELECT` followed by a registry insert is insufficient. SQLite can share the
log database's write transaction (`BEGIN IMMEDIATE`); PostgreSQL needs a
reviewed common transaction guard covering the registry, command UoW, and
any direct event-store writer. Choose and prove this mechanism before coding
the provisioning operation; do not pretend the current adapters provide it.

Preserve credit-only recipient behavior from ADR-0018: no spending permission
on a recipient is required. This proposal does **not** require every credit
recipient to be registered. If a financial write wins the race, ordinary
provisioning conflicts and the stream follows the legacy reconciliation path;
if provisioning wins, the binding is visible before the later credit commits.
If the product instead requires provision-before-any-money, that is an explicit
additional API/lifecycle decision covering claims/admin and credit paths, not
an implicit change hidden in registration.

## Regression matrix required before acceptance

Use existing patterns in `tests/unit/entrypoints/test_account_registry.py`,
`test_http_app.py`, `test_oidc_jwks.py`, and the cross-connection races in
`tests/unit/adapters/test_postgres_tier.py`.

1. Denied old registration makes zero writes; existing owners, explicit account
   grants, and legitimate all-debit postings retain their current access.
2. Anonymous/non-provisioner/admin-only requests cannot create bindings for
   empty, populated, known, or unknown IDs. Invalid bodies disclose no state.
3. A valid workload provisions Alice, not itself. Bob and the workload cannot
   spend solely because provisioning succeeded. Wrong audience/issuer/subject,
   missing/revoked `jti`, invalid lifetime, and missing scope are denied.
4. Exact replays survive credential renewal and restore; changed account,
   allocation, or beneficiary conflicts. Revoked privilege cannot replay.
5. Barrier-start different-owner/exact retries and first-credit/provision races
   on real PostgreSQL connections and SQLite satisfy the two legal orders
   above. Empty-after-money is rejected; acknowledgement is immediately
   visible through another connection's `owner_of`.
6. Backup/restore preserves ownership and provisioning provenance/results;
   same-row-count tampering fails an independent backup-integrity check.

## Alternatives rejected

- First-claim-wins, even on empty streams: no proof of allocation authority.
- Issuer-signed beneficiary entitlement tokens: valid alternative, but not
  the proposed trusted-provisioner-only policy; do not silently combine both.
- Event-backed ownership as the authorization fix: records the assertion but
  does not establish the caller's authority to make it.
- Recipient spending authorization: confuses payment eligibility with consent
  to debit and breaks legitimate payments to others.
- Precheck outside a write transaction: permits stream-creation races.
- Longer result retention as permanent allocation identity: still expires.

## Consequences and acceptance blockers

Ordinary clients lose self-registration. The onboarding authority becomes a
security-critical dependency; absence/unavailability blocks new provisioning,
not existing authorized payments. Legacy mistakes need a controlled resolution
process. New durable provenance has storage/privacy/backup costs.

Before implementing the proposed new provisioning operation, confirm:
(1) allocation authority and stable identity
namespace; (2) denial/cutover and the new route; (3) issuer configuration and
revocation bound; (4) cross-writer serialization mechanism and migration; and
(5) the regression plan. Record acceptance explicitly, update OpenAPI/runbook/
threat model/changelog, and run both storage tiers plus build/security gates.
No production credentials, data inspection, ownership backfill, deployment,
or production secret rotation is authorized by this proposal.
