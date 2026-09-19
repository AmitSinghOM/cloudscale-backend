# ADR-0012: Stream snapshots as a verified cache over the event log

**Status:** Accepted
**Date:** 2026-09-19
**Enforced by:** `tests/unit/domain/test_snapshots.py` (fold-from-snapshot
equals full fold at every cut point over the fixture corpus and over
Hypothesis-generated streams; state-version mismatch is ignored);
`tests/unit/adapters/test_sqlite_command_uow.py::test_snapshot_*` and
`tests/unit/adapters/test_postgres_command_uow.py::test_snapshot_*`
(snapshot written every N events in the command transaction, anchor
mismatch discards the snapshot, monotonic upsert never moves a snapshot
backwards, `CLOUDSCALE_SNAPSHOT_EVERY=0` disables);
`tests/unit/adapters/test_postgres_migrations.py` (schema parity for
`stream_snapshots`, `0005` downgrade); `tests/unit/scripts/test_snapshots_cli.py`
(operator tool on both tiers); `tests/architecture/test_configuration_docs.py`
(`CLOUDSCALE_SNAPSHOT_EVERY` documented). Depth benchmark:
`scripts/bench_depth.py`, numbers below.

## Context

`fold_stream` in both command units of work reads every row of a stream
(`SELECT … WHERE stream = ? ORDER BY seq`) and folds all of them before
every decision. The cost is linear in stream depth, and Phase 3 measured
what that means on the legacy `cqrs/` handler: 0.084 ms at depth 1,
5.649 ms at depth 2,000, a 22× throughput collapse (6,980/s → 313/s).
Phase 3 fixed the legacy path with an in-process memoized fold; the
current `execute_command_decision` path never received the fix, and
ADR-0011 doubled the folds per transfer. Holds (ADR-0014) will add state
to the account and more events per hot stream.

Forces:

- The event log is the sole source of record (ADR-0002). Any derived
  state must be recomputable from it and must never be trusted over it.
- `AccountState` will change shape (ADR-0014 adds `held`). A snapshot of
  an older shape must not be folded forward as if it were current.
- Event upcasters (ADR-0009) translate old rows at read time. A snapshot
  taken before an upcaster changed how an old event folds is stale even
  though its `seq` is current.
- Restores from a dump, retention, or a hand edit can leave a snapshot
  pointing past the end of a stream or at a different event than the one
  it summarised. That must be detected, not assumed away.
- Both tiers must behave identically; the snapshot write must not add a
  failure mode to the command path.

## Decision

A `stream_snapshots` table holds at most one row per stream:
`(stream PRIMARY KEY, seq, state_json, state_version, anchor_event_id)`.
The row means: *`state_json` is the fold of this stream's events with
`seq <= seq`, computed by a build whose `AccountState` shape is
`state_version`, and the event at `(stream, seq)` had id `anchor_event_id`.*

**Read.** `fold_stream(account_id)` reads the snapshot row, then the
events with `seq >= snapshot.seq` (the anchor and the tail in one query).
It uses the snapshot only when all three hold: a row exists, its
`state_version == CURRENT_STATE_VERSION`, and the first returned event's
`seq` and `event_id` equal the row's anchor. Otherwise it logs at WARNING
naming the reason and folds the full stream from `seq = 1`. The tail is
upcast exactly as today. `fold_from(state, tail) == fold(all)` is the
contract, tested at every cut point.

**Write.** After the command's events are appended, and inside the same
transaction, if the stream's new version minus the snapshot's `seq` is at
least `CLOUDSCALE_SNAPSHOT_EVERY` (default 100; `0` disables), the unit of
work upserts the snapshot **only if the new `seq` is greater than the
stored one** (`WHERE excluded.seq > stream_snapshots.seq` on SQLite,
`WHERE stream_snapshots.seq < EXCLUDED.seq` on PostgreSQL). A lagging
retry can therefore never move a snapshot backwards. Because the write is
in the command transaction it cannot half-commit; it adds no new failure
mode beyond the transaction's own.

**Evolution.** `CURRENT_STATE_VERSION` lives in `cloudscale/domain/account.py`
next to `AccountState`. It is bumped when the state's fields change **and**
when an upcaster is registered that changes how any existing event folds.
Snapshots are never upcast: a mismatched version is discarded and the
stream is folded once in full, after which a fresh snapshot is written.
The governance test asserts the constant is referenced from the
upcasting module's docstring so the second rule is not forgotten.

**Operations.** `scripts/snapshots.py <sqlite-path|postgresql-dsn>
{stats | drop <stream> | drop --all}` dispatches on the DSN from day one
and, on PostgreSQL, refuses to run against an unmigrated database (the
review-4 rule for `dlq.py`). Dropping is always safe. `RUNBOOK.md` R7:
if a balance disagrees with a full replay, drop the snapshot and re-read;
if the disagreement persists it is not the snapshot.

## Alternatives rejected

- **In-process memoized fold (the Phase 3 fix).** Does not survive a
  restart and is per process, so a two-replica API pays the full fold on
  every failover and on every cold start. Durable snapshots make the cost
  O(N) per command regardless of depth or process lifetime.
- **Snapshot on every append.** Doubles write amplification for no
  measurable read benefit below the interval; the benchmark below shows
  the interval of 100 already flattens latency against depth.
- **Upcasting snapshots when `AccountState` changes.** Derived data does
  not need a migration path; recomputing from the log is always correct
  and happens once per stream.
- **Compacting the event log to a snapshot.** Never. The log is the truth
  (ADR-0002); retention (Phase 5) applies to the outbox, not to `events`.
- **Projection-side snapshots.** The `balances` table already is the
  read-side snapshot; this ADR is about the write-side fold only.

## Consequences

- Easier: command latency is flat against stream depth on both tiers; hot
  accounts (merchants, treasury) no longer degrade the whole service.
- Harder / must be maintained: `CURRENT_STATE_VERSION` is one more thing
  to bump; the "where to change what" table in `docs/ARCHITECTURE.md`
  names it. `scripts/snapshots.py` is one more operator tool and has its
  own tests on both tiers. Alembic `0005` and `schema.py` must stay in
  parity (existing test).
- Stated limitation: a snapshot is a cache. A tampered or stale row is
  detected by anchor and version checks and discarded, never corrected in
  place; the only repair is recomputation from the log.

## Depth benchmark

`scripts/bench_depth.py` executes one `Withdraw(1)` against a stream of
depth *d* after warming it, 200 samples per point, and reports p50/p99
with `CLOUDSCALE_SNAPSHOT_EVERY=0` and `=100`. Numbers recorded at commit
time on the development host (single process, PostgreSQL 17 local):

BENCHMARK_TABLE_PLACEHOLDER
