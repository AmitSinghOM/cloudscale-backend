# ADR-0011: Double-entry Transfer as two postings in one transaction

**Status:** Accepted
**Date:** 2026-09-19
**Enforced by:** `tests/unit/domain/test_transfer.py` (conservation property,
decision rules, envelope round-trip, result postings);
`tests/unit/adapters/test_sqlite_command_uow.py::test_transfer_*` (atomicity,
replay, rejections); `tests/unit/adapters/test_postgres_command_uow.py`
(`test_transfer_commits_both_legs_in_one_transaction`,
`test_opposite_direction_transfers_never_deadlock`,
`test_target_side_race_self_heals_via_retry_on_fresh_fold`);
`tests/unit/adapters/test_postgres_tier.py::test_transfer_legs_project_into_both_balances_on_postgres`;
`tests/unit/entrypoints/test_http_app.py::test_transfer_*` (authorization,
redaction, statuses, consumer round-trip);
`tests/unit/adapters/test_postgres_migrations.py` (schema parity,
`test_downgrade_0004_refuses_while_transfer_legs_exist`); the fixture corpus in
`tests/fixtures/events/` (`TransferDebited.v1`, `TransferCredited.v1`).

## Context

Until now the domain had one command per account stream (`Deposit`,
`Withdraw`) and one event per command. Money could enter or leave an
account but never move between two accounts atomically: a client had to
issue a withdraw and a deposit as separate commands and reconcile the
failure window itself. For a ledger that is the defining gap — every
production ledger this project compares itself to (Formance, TigerBeetle)
is double-entry by construction.

Forces:

- Both account streams live in the same database on both tiers, so a single
  transaction can cover both appends. Introducing a saga or a Transfer
  aggregate would add a failure-window state machine the storage does not
  need today.
- `execute_command_decision` is shared by both tiers and already owns the
  idempotency (`command_id`), optimistic-version and rejection-persistence
  contract. The transfer must inherit that contract, not duplicate it.
- Adding fields to `Deposited`/`Withdrawn` would bump their schema version
  and exercise the upcaster path (ADR-0009) for a change that is not a
  reshaping of those events.
- Two transfers `A→B` and `B→A` racing on PostgreSQL insert the same two
  `(stream, seq)` keys in opposite orders; unordered appends deadlock
  (`40P01`) and one side fails spuriously.
- A stream's `committed_version` is a count of that account's activity;
  returning it for an account the caller cannot read is an oracle.

## Decision

A `Transfer(account_id, target_account_id, amount, expected_version)` is
decided by a pure function over **both** account states and produces two
new events — `TransferDebited` on the source stream and `TransferCredited`
on the target stream — that share a `transfer_id` (the command id) and name
each other as `counterparty`. Both events are appended and the result is
persisted **in one transaction**, through the existing
`execute_command_decision`, so idempotent replay, `command_id_conflict`,
`version_conflict` and persisted domain rejections behave exactly as for
single-account commands. The client supplies `expected_version` for the
source stream only; the target stream is guarded by `UNIQUE (stream, seq)`
and the unit of work's bounded retry on a fresh fold. Events are appended in
ascending account-id order so concurrent opposite-direction transfers wait
on one key instead of deadlocking. `CommandResult` gains an additive
`postings` list (one entry per stream written); `account_id`,
`committed_version` and `event_id` keep meaning the source. The HTTP route
`POST /v1/accounts/{account_id}/transfers` authorizes the **source** only and
includes a posting's `committed_version` only for accounts the caller may
read. The `events` table gains two nullable columns, `transfer_id` and
`counterparty` (Alembic `0004`), whose downgrade refuses while any transfer
row exists.

## Alternatives rejected

- **Reuse `Deposited`/`Withdrawn` with an optional `transfer_id`.** Bumps
  two schema versions and forces a `1→2` upcaster for a field most events
  will never carry; the projection could no longer tell a transfer leg from
  a cash movement without the new field. New event types keep both existing
  shapes frozen at v1.
- **A Transfer aggregate / saga (`transfer:<id>` stream, Initiated → Debited
  → Credited → Failed).** Correct across databases, unnecessary inside one;
  it adds a compensating path that would be dead code on both tiers. The
  events carry `transfer_id` and `counterparty` so a saga can be introduced
  later without rewriting history.
- **Require `expected_version` for both streams.** Makes every transfer into
  a busy account fail at the rate of that account's traffic, for no safety
  gain — the target's invariants (non-negative, within BIGINT) are checked
  from the fresh fold inside the transaction.
- **Return the target's `committed_version` unconditionally.** An activity
  oracle on accounts the caller is not authorized to read.
- **Pending / two-phase transfers (holds), N-leg postings, cross-database
  transfers, a `transfers` read model.** Deferred; `transfer_id` grouping
  already permits N legs, only the command and decision are pairwise.

## Consequences

- Easier: money moves between accounts with one idempotent request; the
  conservation invariant (sum of balances unchanged by any transfer) is a
  property test, not a client responsibility.
- Harder / must be maintained: `apply` and every balance projection know four
  event types, not two; the fixture corpus carries `TransferDebited.v1` and
  `TransferCredited.v1`; the OpenAPI contract, `docs/API_ERRORS.md` (adds
  `same_account`) and `docs/ARCHITECTURE.md` describe the route.
- Stated limitation: the read model is eventual **per account**. Between the
  two projection applications a reader may observe the debit without the
  credit. Conservation holds in the event log at every commit and in the
  projection at every quiescent point, not at every instant.
