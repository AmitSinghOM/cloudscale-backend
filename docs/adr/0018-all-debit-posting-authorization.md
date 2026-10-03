# ADR-0018: Authorize every debit in an N-leg posting set

**Status:** Supersedes ADR-0013 (posting authorization only)
**Date:** 2026-10-03
**Enforced by:** `tests/unit/entrypoints/test_http_app.py::test_postings_require_every_debit_before_persisting`,
`test_postings_denies_later_debit_before_executor_or_balance_lookup`,
`test_postings_validates_shape_before_authorizing_additional_debits`;
`tests/unit/entrypoints/test_account_registry.py::test_postings_accepts_registry_ownership_of_every_additional_debit`.

## Context

ADR-0013 authorizes only the anchor account. A balanced set can debit a
small amount from that account and a larger amount from someone else's
account. Conservation and sufficient-funds checks do not establish consent.
The approved correction extends the existing all-debit authorization pattern
used by reverts (ADR-0015); all other ADR-0013 decisions remain in force.

## Decision

The posting route authenticates and authorizes the anchor, constructs the
pure domain command to validate its shape, then authorizes every additional
debit using the existing account authorization utility before entering the
executor. Registry ownership, issuer-signed account claims, and the admin
scope retain their existing meaning. Credit-only recipients need no spending
authorization. A denial uses the existing generic 403 and writes neither
an event nor a command result. Replays pass the same current authorization
checks before any stored result is read. Per-account committed-version
redaction is unchanged.

## Alternatives rejected

- Anchor-only authorization: balancing the set does not prevent theft from
  other debited accounts.
- Authorizing credits: unnecessarily prevents payments to other parties.
- Authorizing inside the domain or after execution: the domain must remain
  framework-free, and checking after execution is too late.
- Coupling registration redesign to this fix: a separate ownership-policy
  decision must not delay closing the multi-debit authorization gap.

## Consequences

Previously accepted unauthorized multi-debit requests return 403, including
replays made without current access to every debit. Legitimate posting sets,
transfers, holds, event shapes, storage, and schema are unchanged. The route
performs at most one authorization check per debit (16 legs maximum).
This does not fix registration of an existing unregistered account stream,
change token revocation semantics, or implement ADR-0017.
