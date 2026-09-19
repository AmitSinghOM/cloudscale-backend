# ADR-0014: Pending transfers (holds) as reserve, post, void, expire

**Status:** Accepted
**Date:** 2026-09-19
**Enforced by:** `tests/unit/domain/test_holds.py` (available-funds rule,
place/post/partial/void/expire decision rules, `held` fold, conservation
including held funds); `tests/unit/adapters/test_sqlite_command_uow.py::test_hold_*`
and `tests/unit/adapters/test_postgres_command_uow.py::test_hold_*`;
`tests/unit/adapters/test_postgres_tier.py::test_hold_*` (`balances.held`,
`holds` read model); `tests/unit/scripts/test_sweep_holds.py` (deterministic
command id, two sweepers racing release each hold once);
`tests/unit/entrypoints/test_http_app.py::test_hold_*`;
`tests/unit/adapters/test_postgres_migrations.py` (`0006` parity and guard);
`tests/architecture/test_api_errors_doc.py`; fixture corpus
`HoldPlaced.v1`, `HoldReleased.v1`, `TransferDebited.v1` (with `pending_id`).

## Context

A card authorization, a marketplace escrow and a scheduled payout all need
to reserve funds now and settle later. Without holds a client must either
debit immediately and refund on failure (money moves twice, and a refund
can fail) or check available funds and hope no other command lands in
between. TigerBeetle models this as a two-phase transfer: pending → posted |
voided | expired. That is the model this ADR adopts, at the scale of one
database.

Forces:

- `AccountState` is a frozen dataclass of integers and the fold is O(1)
  per event. A per-hold collection on the state would make snapshots
  (ADR-0012) grow with open holds and make the fold shape irregular.
- Nothing in the service runs on a clock except outbox retention. Expiry
  must not make the fold time-dependent, or replay stops being
  deterministic.
- The target of a hold should learn nothing until funds actually move;
  a hold is the payer's business.
- The event log stays the only truth; the set of open holds is a read
  model, not aggregate state.

## Decision

`AccountState` gains `held: int` (sum of the account's open holds as
source); `available = balance - held`. Every debit rule (`Withdraw`,
`Transfer`, `Post`, `Hold`) checks **available** funds. Two new events at
schema version 1: `HoldPlaced(account_id, amount, hold_id, counterparty,
expires_at)` folds `held += amount`; `HoldReleased(account_id, amount,
hold_id, reason)` with `reason ∈ {voided, expired, partial}` folds
`held -= amount`. Posting a hold reuses `TransferDebited` /
`TransferCredited` with `transfer_id = hold_id`; the debit carries a new
nullable `pending_id = hold_id` so the fold applies `held -= amount` as
well as `balance -= amount`. `HELD_SIGN` is a second table beside
`BALANCE_SIGN`; each projected quantity has exactly one table. Commands:
`Hold(account_id, target_account_id, amount, expected_version, ttl_seconds)`
→ `HoldPlaced` (hold id = command id); `PostHold(account_id, hold_id,
expected_version, amount=None)` → the posting pair, plus a
`HoldReleased(reason=partial)` for the remainder when `amount` is less than
held, all in one transaction; `VoidHold(account_id, hold_id,
expected_version)` → `HoldReleased(voided)`. Hold state (open, amount,
expiry, target) is read from the `holds` read model maintained by the
consumer, not from the aggregate.

**Expiry** is an ordinary command. `scripts/sweep_holds.py <db-or-dsn>`
reads open holds past `expires_at` from the read model and executes
`ExpireHold` for each with a deterministic `command_id = uuid5(hold_id,
"expire")`, so two sweepers, or a sweeper racing a `PostHold`, resolve
through the existing `command_results` claim: exactly one wins, the other
sees the stored result or `hold_not_open`. The fold never consults the
clock.

Storage: `events` gains nullable `pending_id` and `expires_at` (Alembic
`0006`, downgrade refuses while hold rows exist); `balances` gains `held`;
new `holds(hold_id PK, source, target, amount, expires_at, state)`. HTTP:
`POST …/{account_id}/holds` (201; `hold_id` in the body),
`POST …/holds/{hold_id}/post`, `POST …/holds/{hold_id}/void`;
`GET …/balance` gains `held` and `available`. New error codes:
`hold_not_found` (404), `hold_not_open` (409), `capture_exceeds_hold` (400).
Authorization is on the source for all three routes; the target's stream is
untouched until post.

## Alternatives rejected

- **Lazy expiry inside the fold.** Makes `fold` depend on `now()`; replay of
  the same log at two times yields two states. Rejected outright.
- **A `holds` dict on `AccountState`.** Snapshots grow with open holds and
  the fold shape stops being fixed; the read model carries per-hold detail
  instead.
- **A separate `HoldPosted` event type.** The debit *is* a transfer debit;
  a nullable `pending_id` keeps `BALANCE_SIGN` the single balance table.
- **Debit at reserve time into a suspense account.** Moves money twice
  and shows the target activity it has no claim to; also breaks the
  per-account conservation reading of the balance.

## Consequences

- Easier: card-style authorize/capture, escrow and scheduled payouts are
  three idempotent requests with no client-side compensation.
- Harder / must be maintained: the fold has one more quantity and
  `CURRENT_STATE_VERSION` (ADR-0012) is bumped; a sweeper is one more
  operator process and appears in `RUNBOOK.md`; the fixture corpus grows by
  two types and one variant.
- Stated limitation: `expires_at` is honoured to sweeper cadence, not to
  the second; a hold is enforceable only once the sweeper has run. Holds
  with N legs and target consent (escrow with acceptance) are deferred.
