# ADR-0014: Pending transfers (holds) as reserve, post, void, expire

**Status:** Accepted
**Date:** 2026-09-19
**Enforced by:** `tests/unit/domain/test_holds.py` (available-funds rule,
open-hold derivation, place/post/partial/void/expire decision rules, `held`
fold, envelope round trip, Hypothesis lifecycle property: conservation and
`held == sum(open holds)` at every step);
`tests/unit/adapters/test_sqlite_command_uow.py` (`test_hold_*`,
`test_post_hold_*`, `test_partial_capture_*`, `test_void_and_expire_*`) and
`tests/unit/adapters/test_postgres_command_uow.py`
(`test_hold_lifecycle_on_postgres_and_read_model`,
`test_concurrent_post_and_void_of_one_hold_resolve_to_exactly_one_winner`);
`tests/unit/scripts/test_sweep_holds.py` (deterministic command id,
idempotent sweep, two sweepers racing leave exactly one
`HoldReleased(expired)` per hold in the log);
`tests/unit/entrypoints/test_http_app.py::test_hold_*`;
`tests/unit/adapters/test_postgres_migrations.py::test_downgrade_0006_refuses_while_hold_events_exist`
and the schema-parity test; `tests/architecture/test_api_errors_docs.py`;
fixture corpus `HoldPlaced.v1`, `HoldReleased.v1`, `HoldPosted.v1`.

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
`Transfer`, `Post`, `Hold`) checks **available** funds. Three new event
types at schema version 1, all on the source stream: `HoldPlaced(account_id,
amount, hold_id, counterparty, expires_at)` folds `held += amount`;
`HoldReleased(account_id, amount, hold_id, reason)` with `reason ∈ {voided,
expired, partial}` folds `held -= amount`; `HoldPosted(account_id, amount,
hold_id, counterparty)` folds `balance -= amount; held -= amount` and is
paired with a `TransferCredited` on the target whose `transfer_id` is the
`hold_id`. `HELD_SIGN` is a second table beside `BALANCE_SIGN`; each
projected quantity has exactly one table and the fold applies both.
`CURRENT_STATE_VERSION` (ADR-0012) becomes 2. Commands:
`Hold(account_id, target_account_id, amount, expected_version, expires_at)`
→ `HoldPlaced` (hold id = command id; `expires_at` is absolute UTC text
produced by the HTTP layer from `ttl_seconds`, bounded by
`CLOUDSCALE_HOLD_MAX_TTL_SECONDS`); `PostHold(account_id, hold_id,
expected_version, amount=None)` → `HoldPosted` + `TransferCredited`, plus a
`HoldReleased(partial)` for the remainder when `amount` is less than held,
all in one transaction; `VoidHold` → `HoldReleased(voided)`; `ExpireHold` →
`HoldReleased(expired)`, refused before `expires_at`.

**The decision reads the hold from the log, never from the read model.**
`CommandDecisionStorage.open_hold(account_id, hold_id)` derives the open
hold from the source stream's own `Hold*` events with that id, inside the
command transaction (`open_hold_from_events` in the domain). An eventual
read model could lag a just-placed hold or a just-completed post; the log
cannot. `PostHold` and `ExpireHold` consult the decision clock (the same
`now` that stamps `occurred_at`) once, at decision time; the *fold* never
reads a clock, so replay stays deterministic.

**Expiry** is an ordinary command. `scripts/sweep_holds.py` reads open
holds past `expires_at` from the `holds` read model and executes
`ExpireHold` for each with a deterministic `command_id = uuid5(namespace,
hold_id)`, so two sweepers, or a sweeper racing a `PostHold`, resolve
through the existing `command_results` claim: the log carries exactly one
`HoldReleased(expired)` per hold. A concurrent sweeper that folded the same
version issues a byte-identical command and receives the stored ACCEPTED
result as a replay; one that folded a different version gets
`command_id_conflict` or `version_conflict`; one that arrives after a post
gets `hold_not_open`.

Storage: `events` gains nullable `expires_at` and `release_reason`; a hold's
id is stored in the existing `transfer_id` column (it becomes the posting's
transfer id) with a partial index on `(stream, transfer_id)` serving the
derivation (Alembic `0006`, downgrade refuses while hold events exist);
`balances` gains `held`; new `holds(hold_id PK, source, target, amount,
expires_at, state)` maintained by every projection. HTTP:
`POST …/{account_id}/holds` (201; the `command_id` is the `hold_id`),
`POST …/holds/{hold_id}/post`, `POST …/holds/{hold_id}/void`;
`GET …/balance` gains `held` and `available`. New error codes (all 400):
`hold_not_open`, `hold_expired`, `hold_not_expired`, `capture_exceeds_hold`,
`invalid_expiry`. Authorization is on the source for all three routes; the
target's stream is untouched until post and its `committed_version` is
redacted unless the caller may read it. `CommandResult.postings` stays one
per stream: when a partial capture writes the source twice, the posting
names the source's last event.

## Alternatives rejected

- **Lazy expiry inside the fold.** Makes `fold` depend on `now()`; replay of
  the same log at two times yields two states. Rejected outright.
- **A `holds` dict on `AccountState`.** Snapshots grow with open holds and
  the fold shape stops being fixed; the read model carries per-hold detail
  instead.
- **A separate `HoldPosted` event type versus reusing `TransferDebited`
  with a `pending_id`.** Reusing the transfer debit would add a key to its
  v1 payload, which the envelope validates exactly (ADR-0011 chose new
  types over shape changes for the same reason); `HoldPosted` keeps
  `TransferDebited.v1` frozen and lets `HELD_SIGN` stay a plain sign table.
- **Reading the hold from the `holds` read model in the decision.** The
  read model is eventual; a post right after a place could see no hold, and
  two racing resolutions could both see it open. The decision derives it
  from the source stream's events inside the transaction instead.
- **Debit at reserve time into a suspense account.** Moves money twice
  and shows the target activity it has no claim to; also breaks the
  per-account conservation reading of the balance.

## Consequences

- Easier: card-style authorize/capture, escrow and scheduled payouts are
  three idempotent requests with no client-side compensation.
- Harder / must be maintained: the fold has one more quantity and
  `CURRENT_STATE_VERSION` (ADR-0012) is bumped to 2, so every existing
  snapshot is discarded and refolded once on first read after deploy; a
  sweeper is one more operator process and appears in `RUNBOOK.md`; the
  fixture corpus grows by three types; every balance projection carries
  `held` and the `holds` table.
- Stated limitation: `expires_at` is honoured to sweeper cadence, not to
  the second; a hold is enforceable only once the sweeper has run. Holds
  with N legs and target consent (escrow with acceptance) are deferred.
