# ADR-0013: N-leg balanced postings on the transfer_id grouping

**Status:** Accepted
**Date:** 2026-09-19
**Enforced by:** `tests/unit/domain/test_postings.py` (command validation:
balanced, no duplicate account, anchor debited, leg bounds; decision rules;
conservation property over random balanced posting sets);
`tests/unit/adapters/test_sqlite_command_uow.py::test_post_*` (all legs in one
transaction, replay, all-or-nothing rejection);
`tests/unit/adapters/test_postgres_command_uow.py::test_three_way_cyclic_postings_never_deadlock`;
`tests/unit/entrypoints/test_http_app.py::test_post_postings_*` (route,
statuses, redaction, error codes); `tests/architecture/test_api_errors_doc.py`
(`unbalanced`, `too_many_legs`, `duplicate_account` documented);
`tests/unit/entrypoints/test_openapi_contract.py`.

## Context

ADR-0011 made a transfer two postings in one transaction sharing a
`transfer_id`, appended in ascending account-id order. The storage and the
execution loop are already N-ary: `_decide_legs` returns a list,
`execute_command_decision` appends each leg, and the `events` row carries
`transfer_id` and `counterparty`. Only `Transfer` and `decide_transfer` are
pairwise. Production ledgers (Formance: atomic multi-posting transactions)
treat a posting set, not a pair, as the unit of money movement: a fee split,
a payout to several parties, or a settlement across a clearing account are
one intent and must commit or fail together.

Forces:

- Double-entry means every accepted command conserves money. That must be
  a property of the command, not of the caller's arithmetic.
- The single-asset, minor-units model stays; multi-asset is a different ADR.
- The anchor account (authorization and `expected_version`) must be one
  whose money leaves, or a caller could move funds between two accounts it
  does not own by naming its own account as a zero-effect leg.
- More streams per command means more chances for the fresh-fold retry to
  be exhausted on hot accounts; contention must be visible to operators.

## Decision

A `Post(account_id, postings, expected_version)` command carries 2 to
`MAX_LEGS = 16` `Leg(account_id, amount, direction)` legs. Command
validation rejects: unequal debit and credit totals (`unbalanced`), any
account named twice (`duplicate_account`), more than `MAX_LEGS` legs
(`too_many_legs`), fewer than two legs, and an anchor that is not among the
debited accounts (`anchor_not_debited`). `decide_postings(states, command)`
folds every named stream, applies `_require_funds` to each debited account
and `_require_credit_fits` to each credited account, and `_require_next_version`
to all; one failing leg rejects the whole command, persisted under the
`command_id` exactly as a rejected transfer is. The events are the existing
`TransferDebited` / `TransferCredited` at schema version 1; for N > 2,
`counterparty` on every non-anchor leg is the anchor account (the payer),
and on the anchor's own leg it is the largest credited payee (first in leg
order on ties) — an event may not name itself — which is what a statement
line shows. Legs are appended in ascending account-id order — the total
order that prevents lock cycles of any length, not only pairs.
`Transfer` becomes the two-leg convenience and is unchanged for clients.
`POST /v1/accounts/{account_id}/postings` authorizes the anchor and redacts
`committed_version` per posting as ADR-0011 does. A `command_retries_total`
counter records fresh-fold retries so hot-account contention is measured
before clients see `version_conflict`.

## Alternatives rejected

- **Unbalanced batches (a list of independent debits/credits).** Not
  double-entry; conservation would depend on the caller. A batch API is a
  separate concern and is not this ADR.
- **A new event type per leg kind (`PostingDebited`).** Adds a third
  balance sign and a fixture pair for no new information; the existing two
  types already say direction and grouping.
- **Rename `counterparty` for N > 2.** Would bump both transfer event
  schemas; the field's meaning is documented instead.
- **Per-leg `expected_version`.** Same reasoning as ADR-0011: the
  non-anchor streams are guarded by `UNIQUE (stream, seq)` and the retry.

## Consequences

- Easier: a fee split or multi-party payout is one idempotent request with
  conservation guaranteed by construction; `Transfer` needs no change.
- Harder / must be maintained: three new error codes in `docs/API_ERRORS.md`;
  the OpenAPI contract gains a route; `MAX_LEGS` is a public limit and is
  documented as such.
- Stated limitation: all legs are in one database transaction, so a
  posting set spanning databases is out of scope (the saga path ADR-0011
  deferred remains deferred).
