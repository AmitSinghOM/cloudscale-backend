# ADR-0017: Account registration as an event on the account stream

**Status:** Proposed
**Date:** 2026-09-20
**Enforced by (once accepted):** `tests/unit/domain/test_register_account.py`
(fold sets `owner_subject` once; a second registration is `account_taken`; the
fixture corpus gains `AccountRegistered.v1`); `tests/unit/adapters/test_*_command_uow.py::test_register_*`
(two registrations racing on PostgreSQL leave exactly one `AccountRegistered`;
the `accounts` row is written in the same transaction as the event and is
visible to `owner_of` before the consumer runs); `tests/unit/entrypoints/test_http_app.py::test_register_*`
(`POST /v1/accounts` keeps 201 / 200 / 409 byte-for-byte; a retry is a replay);
`tests/unit/adapters/test_postgres_migrations.py::test_downgrade_0008_*`;
`tests/architecture/test_table_classification.py` (`accounts` in `DERIVED`,
`SYSTEM_OF_RECORD == {events, event_envelopes, command_results}`); the
restore drill (ADR-0016) truncating and rebuilding `accounts`;
`scripts/backfill_registrations.py` idempotent under a deterministic command id.

## Context

ADR-0002 says the event log is the sole source of record. ADR-0016 had to
classify `accounts` as **system of record** anyway: `POST /v1/accounts`
writes an `INSERT … ON CONFLICT DO NOTHING` straight into the table, so
ownership — the fact that decides who may move money on an account — lives
outside the log, has no envelope, no correlation id, no audit trail beyond a
log line, and would be lost by a restore that carried only the log. That
contradiction is the one exception to the charter's first data principle,
and it sits on the authorization path.

Forces:

- Authorization reads `owner_of(account_id)` on **every** request. It must
  be strongly consistent: a client that receives 201 from registration and
  immediately issues a command must not get 403 while a consumer catches up.
- Two callers may register the same id at once; exactly one may win, and
  the loser must learn it deterministically.
- Existing deployments have `accounts` rows with no event behind them; a
  design that cannot be backfilled idempotently is not deployable.
- The subject (`sub` claim) would become immutable in the log. It is a
  pseudonymous identifier today, already stored in `accounts`.
- Registration is optional: claims-based and admin authorization work on
  unregistered accounts (Phase 5), and a stream may already have events
  before anyone registers it.

## Decision

Registration is a **command on the account stream**. `RegisterAccount(account_id,
owner_subject, expected_version)` goes through the same idempotent unit of
work as every other command and appends `AccountRegistered.v1(account_id,
owner_subject, registered_at)`; `registered_at` comes from the decision
clock, once, as `HoldPlaced.expires_at` does. The fold sets
`AccountState.owner_subject` the first time and never again; a second
registration of an owned account is the persisted rejection `account_taken`
(409), and the same subject registering again is an ordinary replay (200).
Both sign tables carry the type with sign 0. The stream may already hold
events (the event lands at version *n*, not necessarily 1) and may never be
registered at all; nothing else in the fold depends on ownership.

**The `accounts` table stays, reclassified `DERIVED`, and is written twice
by design.** The unit of work writes the row **in the same transaction as
the append** — a transactional read model, exactly as `stream_snapshots`
is — so `owner_of` remains one primary-key read that is strongly consistent
with the log; and every projection also maintains it idempotently from
`AccountRegistered` (`INSERT … ON CONFLICT DO NOTHING`), so a rebuild after
truncation restores it. The ADR-0016 drill truncates it with the rest and
the consumer must bring it back. `SYSTEM_OF_RECORD` becomes exactly
`{events, event_envelopes, command_results}`; `command_results` stays because
a persisted rejection has no event.

**Concurrency needs nothing new.** Two registrations of one id both write
the account stream, collide on `UNIQUE (stream, seq)`, and the loser
refolds, sees `owner_subject` set, and is rejected `account_taken` — the
mechanism ADR-0011, 0014 and 0015 already rely on. The `ON CONFLICT` dance
in both registries is deleted.

**HTTP contract unchanged.** `POST /v1/accounts` keeps `{account_id}` and
201 / 200 / 409 with the same bodies. The route derives a deterministic
command id, `uuid5(REGISTRATION_NAMESPACE, f"{account_id}:{subject}")`, so a
network retry is a replay without the client learning about command ids;
different subjects derive different ids and the second is the persisted
`account_taken`. `expected_version` is the current fold's version, read in
the route as the sweeper does. The audit record is unchanged.

**Backfill.** Alembic `0008_account_registered` adds nothing to `events`
(the row shape already fits: `type`, `account_id`, `counterparty` carries the
subject) and refuses downgrade while any `AccountRegistered` row exists.
`scripts/backfill_registrations.py <dsn-or-path>` appends one
`AccountRegistered` per existing `accounts` row through the command path
under `uuid5(BACKFILL_NAMESPACE, account_id)`, so it is idempotent and two
operators running it race safely. **Until it has run, the restore drill
fails** on `rebuilt_accounts_equal_dump` — the drill telling the operator
the backfill is outstanding is the intended behaviour, and the RUNBOOK
release step says so. `registered_at` for backfilled rows is the original
`accounts.created_at`, passed explicitly; the event carries a
`backfilled: true` flag so the two provenances are never confused.

## Alternatives rejected

- **Authorize from the eventually consistent read model.** 201 followed by
  403 until the consumer runs; unacceptable on the authorization path.
- **Authorize by folding the stream on every request.** Strongly consistent
  and O(1) with snapshots (ADR-0012), but it puts a fold plus a possible
  snapshot write on every read request and makes authorization latency
  depend on stream depth between snapshots. The transactional row keeps the
  primary-key read.
- **Keep `accounts` as system of record and document the exception.** The
  status quo, now written down by ADR-0016. It leaves ownership outside the
  envelope/correlation/replay machinery and outside the log-only restore.
- **A separate `ownership` stream per account.** Doubles the streams and
  gives the anchor rule of every multi-stream command a second stream to
  order against, for no benefit: ownership is a fact about the account.
- **Migration-time backfill inside Alembic `0008`.** Migrations must not
  write events (ADR-0007 keeps schema and data changes apart); a script
  through the command path is idempotent, auditable and re-runnable.
- **Dropping `accounts` entirely.** Possible, but every authorization check
  would then fold; rejected with the second alternative.
- **Registration transfers (`owner` reassignment).** A different intent with
  its own authorization question; not this ADR.

## Consequences

- Easier: the log is the sole source of record with no exception; ownership
  has an envelope, a correlation id, and replays; the same-id race resolves
  by the mechanism every other command uses; a restore that carries only
  the log restores ownership.
- Harder / must be maintained: one more event type and fixture; a read model
  written on two paths (transactional and consumer) that must agree — the
  drill compares them; a backfill step in the release runbook; the subject
  becomes immutable in the log (a pseudonymous id already stored today; if
  erasure of subjects ever becomes a requirement, that is a
  crypto-shredding or subject-indirection ADR, and this one names it).
- Stated limitation: a deployment that upgrades and does not run the
  backfill keeps working (authorization still reads `accounts`) but its
  restore drill fails until it does. That is the design.
