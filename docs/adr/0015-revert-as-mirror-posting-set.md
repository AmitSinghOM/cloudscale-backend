# ADR-0015: Revert as a mirror posting set linked to the original

**Status:** Accepted
**Date:** 2026-09-20
**Enforced by:** `tests/unit/domain/test_revert.py` (mirror derivation for
two-leg, N-leg and posted-hold originals; `not_revertible` for cash and
hold-lifecycle ids; anchor must be credited; Hypothesis conservation with
random reverts and revert-of-revert restoring every balance);
`tests/unit/adapters/test_sqlite_command_uow.py::test_revert_*` and
`tests/unit/adapters/test_postgres_command_uow.py::test_revert_*` (atomic
mirror, payee spent the money, `already_reverted` persisted, two reverts
racing leave one reversal set, revert racing a payee's withdraw never goes
negative); `tests/unit/entrypoints/test_http_app.py::test_revert_*` and
`test_get_transfer_*` (201 + postings, 403 for a payer-only token, admin
succeeds, `reverted_by` in the read model);
`tests/unit/adapters/test_postgres_migrations.py::test_downgrade_0007_*`;
`tests/architecture/test_api_errors_docs.py`; fixture corpus
`ReversalDebited.v1`, `ReversalCredited.v1`.

## Context

Money that has moved sometimes has to move back: a refund, a mis-keyed
payout, a disputed fee split. Today the only way is a client-issued reverse
`Transfer` or `Post`, which loses the link to what it undoes, cannot be
told apart from a new payment, and lets a second operator reverse the same
payment twice. Formance's ledger has `revert` as a first-class primitive:
a mirror transaction is appended and the original is marked
`reverted`/`revertedAt`. Holds (ADR-0014) cover money that has *not* moved
yet; this ADR covers money that has.

Forces:

- The log is the sole source of record and is append-only (ADR-0002).
  "Reverted" must be derivable from the log, never written onto the
  original rows.
- The aggregate forbids a negative balance and conservation is a property
  (ADR-0011); a revert must respect both, so a payee who has since spent
  the money cannot be reverted into debt.
- A revert debits accounts the caller may not own. Per-account principals
  exist here; the authorization rule must not allow a payer to claw back a
  payment unilaterally.
- Two operators may try to revert the same payment at once.

## Decision

A `Revert(account_id, transfer_id, expected_version)` appends a **new
balanced posting set that mirrors the committed set identified by
`transfer_id`**: every original credit becomes a `ReversalDebited` on that
account and every original debit a `ReversalCredited`, with the exact
original amounts, all in one transaction through `execute_command_decision`,
sorted by account id as every multi-stream command is. Both new event types
are schema version 1 and carry `reverts` (the original's `transfer_id`)
alongside their own `transfer_id` (the revert's command id). The original
rows are untouched. Revertible: a `Transfer`, a `Post`, a posted hold
(`HoldPosted` + `TransferCredited`), and a revert (a revert of a revert
restores the original's effect — no special case). Not revertible:
`Deposit`/`Withdraw` (no grouping; their reversal is the opposite cash
command) and hold placements/releases (they moved nothing); these reject
with `not_revertible`.

The decision reads the log inside the transaction, never the read model:
`legs_of(transfer_id)` returns every movement row across streams and
`reverted_by(transfer_id)` the first reversal that names it. A second
revert is rejected with `already_reverted`. Under concurrency no new lock is
needed: every revert writes the anchor stream, so two reverts of one
original collide on `UNIQUE (stream, seq)` there, the loser refolds and now
sees `reverted_by`, and is rejected — persisted under its own command id
like every rejection.

Each `ReversalDebited` is checked against **available** funds and each
`ReversalCredited` for headroom; one failing leg rejects the whole revert
(`insufficient_funds`). There is no `force`: a payee who spent the money owes
it, which is a receivable, not a negative ledger balance. The anchor
(`account_id`, whose `expected_version` the caller supplies) must be one of
the accounts the revert **credits** — money returns to it — the mirror of
`Post`'s anchor-must-be-debited rule; otherwise `anchor_not_credited`.

**Authorization** is on the debit set, not the anchor: the caller must be
authorized on every account the revert debits (each original payee) or hold
the admin scope. A payer-only token reverting a two-leg transfer is a 403
(not persisted; it is not a domain decision). For a fee split this is in
practice an operator action. The audit record names the original
`transfer_id` and every stream written.

**Storage.** `events` gains a nullable `reverts` column written from
`event_row_fields`, an index on `(transfer_id)` alone (the existing one is
`(stream, transfer_id)`) for `legs_of`, and a partial index on `reverts`
for `reverted_by` (Alembic `0007`; downgrade refuses while any `Reversal*`
row exists). Projections gain nothing but the two sign-table rows.

**Read model.** A `transfers` table — `(transfer_id PK, kind, anchor,
legs_json, reverted_by NULL)` — maintained by every projection from the
movement events, so "is this payment reverted?" is a primary-key read.
`GET /v1/transfers/{transfer_id}` returns it to a caller authorized on at
least one leg's account, with amounts redacted on legs whose account the
caller may not read (the ADR-0011 rule extended).

**HTTP.** `POST /v1/accounts/{account_id}/transfers/{transfer_id}/revert`
with `{command_id, expected_version}` → 201 with one posting per stream.
Errors: `400 not_revertible`, `400 anchor_not_credited`, `409
already_reverted`, `422 insufficient_funds`, plus the shared 401/403/409/503.

## Alternatives rejected

- **Marking the original rows reverted.** Formance's shape; violates the
  append-only log. The flag lives in the `transfers` read model, derived.
- **`force` / negative balances on revert.** Breaks the aggregate invariant
  and the conservation property; a shortfall is a receivable.
- **A `reverts` key on `TransferDebited`/`TransferCredited`.** Alters frozen
  v1 payloads; new types instead (as `HoldPosted` was).
- **Anchor-only authorization.** Unilateral clawback by the payer.
- **Partial reverts.** A different amount is a different intent; use
  `Transfer` or `Post`.
- **Reverting `Deposit`/`Withdraw`.** No `transfer_id` to mirror; the
  opposite command is the reversal and already exists.

## Consequences

- Easier: a refund or mis-payment is one idempotent, linked, exactly-once
  request; support can answer "was this reverted, by what, when" from a
  primary-key read.
- Harder / must be maintained: two more event types and fixtures; a second
  read model beyond balances, kept by all three projections; two more
  indexes on `events`; the authorization rule for reverts differs from every
  other route and must stay documented in `API_ERRORS.md` and the threat
  model.
- Stated limitation: a revert can fail because a payee moved the money on.
  That is the design, not a gap; the caller gets `insufficient_funds` and
  the original stays unreverted.
