# ADR-0016: Restore drill as an executable gate over a classified schema

**Status:** Proposed
**Date:** 2026-09-20
**Enforced by (once accepted):**
`tests/architecture/test_table_classification.py` (every table in a
migrated PostgreSQL database — and in the SQLite consumer's DDL — belongs to
exactly one of `SYSTEM_OF_RECORD`, `DERIVED`, `EPHEMERAL`; an unclassified
table fails the build); `tests/architecture/test_runbook_r7_matches_schema.py`
(RUNBOOK R7 names every table in each class and no other);
`tests/unit/adapters/test_postgres_restore_drill.py` and
`tests/unit/adapters/test_sqlite_restore_drill.py` (seeded mixed workload →
dump → restore into a fresh database → `migrate current` equals
`CURRENT_REVISION` → adapters start in `migrations` mode → every `DERIVED`
table truncated → consumer rebuilds → balances, `held`, open holds and
`transfers` equal both a full fold of the restored log and the pre-dump
read models; every pass criterion fixed in code); CI job `restore-drill`
producing `evidence/<sha>/restore-drill/report.json` on every push to
`main` and every tag; `scripts/restore_drill.py` exit codes (0 pass, 3 any
comparison failed, 2 tooling/version mismatch).

## Context

ADR-0002 makes the event log the sole source of record. The value of that
decision is recoverability: lose every read model and the service comes
back from the log. Today that claim is asserted, not tested:

- `docs/LONGEVITY.md` §4 labels "every other table is derived and
  rebuildable (RUNBOOK R7 lists which)" **Enforced**, yet no test on either
  tier truncates the derived tables and rebuilds them. By the charter's own
  definition this is a *policy* item mislabelled, and it survived the
  v0.6.0, v0.7.0, v0.8.0 and v0.9.0 charter reviews.
- RUNBOOK R7's list of derived tables was written at v0.5.0 and names
  `balances`, `processed_events`, `consumer_offset`, `outbox`,
  `stream_snapshots`. It does not name `holds` (ADR-0014), `transfers` or
  `transfer_legs` (ADR-0015), added in the last two releases. A hand-kept
  list drifted twice in one week; an annual drill would find that a year
  late.
- R7 says the system of record is `events` + `command_results` +
  `event_envelopes`. It is also `accounts`: ownership registrations
  (`POST /v1/accounts`) are written outside the log and cannot be rebuilt
  from it. R7 files that under "small; include in backups", which is true
  and hides the classification.
- `command_results` is system of record for a reason that must be stated:
  a persisted rejection has no event, and a persisted acceptance is what
  makes a client's retry a replay instead of a second deposit. A restore
  that drops it turns every in-flight retry into a duplicate command.
- The charter's Annual restore drill (§5, first due 2026-Q4) is the next
  dated policy item, and its procedure is prose. The comparisons against
  Formance, Marten and TigerBeetle name single-database recovery as this
  service's structural gap; the honest answer for a single database is a
  measured RPO and RTO, and there is no measurement.

Forces: the drill must prove *derivability* (rebuild, not just restore);
it must be cheap enough to run on every release, or it will not run; the
classification must be where a new table cannot avoid it; and the annual
production-scale drill stays a human act, because only a person has a
production dump.

## Decision

Every table is classified **in code**, and the restore drill is an
**executable script with pass criteria fixed in code**, run by CI on every
push to `main` and every tag, with its report committed as release evidence.

**Classification.** `cloudscale/adapters/postgres/schema.py` declares three
frozensets that must partition the tables of a migrated database exactly:

- `SYSTEM_OF_RECORD = {events, event_envelopes, command_results, accounts}`
  — lost data is lost money, identity or idempotency; the backup is these.
- `DERIVED = {outbox, stream_snapshots, balances, holds, transfers,
  transfer_legs, processed_events, consumer_offset, dead_letters}` —
  rebuilt from `events` by the consumer and the relay; `stream_snapshots`
  by the next fold (ADR-0012). `events.published` is a derived *column* on
  a source-of-record table and is reset with the outbox.
- `EPHEMERAL = {rate_limit_buckets}` — neither backed up nor rebuilt; a
  restart forgives a budget.

A PG-gated test compares the union against `information_schema` of a
migrated database (`alembic_version` excepted by name — it is Alembic's, not
ours, and the drill verifies it through `migrate current`); a SQLite test
compares against the consumer's DDL. Adding a table without classifying it
fails the build. RUNBOOK R7 is checked
against the same sets, so the list can no longer drift.

**The drill.** `scripts/restore_drill.py <dsn-or-path>` with DSN dispatch
from the first line (the review-4 lesson from `dlq.py`), in two modes:

1. *Seeded* (CI, tests): create a throwaway database, migrate it, run a
   deterministic mixed workload through the real command path — deposits,
   withdraws, transfers, N-leg posting sets, holds through place/post/
   partial/void/expire, reverts — with the consumer draining, so every event
   type in the fixture corpus is present and every read model is populated.
2. *Production-like* (the annual human drill): point at an existing dump.

Then, identically in both modes: record the log tail (`max(events.id)`,
row count) and a **full fold of every stream** from `events` as the
authoritative expectation; `pg_dump --format=custom` the whole database
(a consistent snapshot; derived tables are cheap to carry and make a
straight restore fast); `pg_restore` into a fresh uniquely named database;
`python -m cloudscale.entrypoints.migrate current` must print
`CURRENT_REVISION`; construct the adapters in `migrations` schema mode (they
must not raise); truncate every `DERIVED` table and reset
`consumer_offset`; run the resilient consumer to drain; then compare.
Pass criteria, all fixed in code: rebuilt `balances.balance` and `held`
equal the full fold for every stream; the open-hold set derived from the log
equals `holds WHERE state = 'open'`; `transfers`/`transfer_legs` equal the
pre-dump rows; `sum(balance)` before equals after (conservation, ADR-0011);
`processed_events` count equals events applied; `dead_letters` after is a
subset of before (a poison event dead-letters again; a transient failure
does not, and that is correct, so equality is not required). On SQLite the
"dump" is the backup API into a fresh file and the same steps follow.

**Evidence.** The report goes to `evidence/<sha>/restore-drill/report.json`:
duration of dump, restore, migrate-verify, rebuild (the RTO components),
event count and rebuild events/s, dump size, the classification used, and
an `rpo_events` field — the difference between the source's log tail at
report time and the dump's — which is 0 in seeded mode and exists so that a
production drill records real loss instead of implying none. Failing
evidence is never committed (charter §7). The release PR commits the tag's
report, as gate runs are committed today.

**CI.** A `restore-drill` job in the existing workflow, against the
`postgres:17` service container, with `postgresql-client` pinned to the
server's major (`pg_dump` refuses a newer server; the script checks
`pg_dump --version` against `SHOW server_version_num` and exits 2 on
mismatch rather than fail obscurely). The report is uploaded as an artifact
on every run.

**Charter and SLO.** `LONGEVITY.md` §4's rebuildability item becomes
truthfully *Enforced* (by the classification test and the drill test); §5's
restore drill splits into *Enforced* (seeded scale, every release) and
*Policy* (production scale, annual, first 2026-Q4, run with this script and
committed the same way). `docs/SLO.md` gains a Recovery section stating RPO
as "the age of the last dump" — the deployment chooses the interval, and
RUNBOOK D-item recommends WAL archiving for point-in-time recovery — and
RTO as the drill's measured rebuild rate multiplied by log size, labelled
*measured at seeded scale, not promised* until a production-scale report
exists.

## Alternatives rejected

- **Keep R7 as documentation and the drill annual.** The list drifted
  twice in one week; a year of drift discovered during a real restore is
  the failure mode this ADR exists to prevent.
- **Restore the dump and stop (no rebuild).** Proves `pg_restore` works,
  which PostgreSQL already promises. The claim under test is that derived
  tables are derivable; only truncating and rebuilding tests it.
- **Streaming replication or a hot standby as the recovery story.** That is
  availability, not recovery: a standby replicates a bad migration or a
  corrupting bug instantly and still needs a restore behind it. It is also
  deployment infrastructure outside this repository. It remains the named
  structural gap and deserves its own ADR when there is a deployment to
  attach it to.
- **Dump only the `SYSTEM_OF_RECORD` tables.** Smaller and faster, but a
  straight restore would then always pay a full rebuild. The whole-database
  dump keeps restores fast; the drill truncates on top of it to prove the
  rebuild anyway. The classification tells an operator what they may *not*
  lose, not what to exclude.
- **Rebuild from `stream_snapshots`.** A cache, refused by ADR-0012; the
  drill truncates it with the rest and lets the next fold rewrite it.
- **Make `accounts` an `AccountRegistered` event now, so the log is truly
  the sole source of record.** The right end state and a candidate
  ADR-0017; it changes the registration write path and ADR-0002's wording,
  which is a separate decision. Until then `accounts` is classified as
  system of record and the drill backs it up as such.
- **A pytest-only drill without a script.** Tests cannot be pointed at a
  production dump; the annual human drill needs the same code path as CI
  or the two will diverge as R7 did.

## Consequences

- Easier: "what must be in the backup" is a frozenset, not a paragraph; a
  new read model cannot ship unclassified; every release carries a measured
  restore time; the annual drill is one command whose report is comparable
  to CI's; the charter's §4 label is true.
- Harder / must be maintained: one more CI job (a seeded drill is expected
  to take one to two minutes); pinned PostgreSQL client tooling in CI; an
  evidence directory per release; the `DERIVED` set must be extended with
  every read model and the drill's comparison extended to cover it — the
  classification test makes forgetting the first impossible, and the
  R7 drift test makes forgetting the runbook impossible, but the comparison
  is a reviewer's job and the ARCHITECTURE "add a read model" procedure
  gains that step.
- Stated limitation: a seeded drill measures the mechanism, not production
  scale. RTO at production size is known only after the first
  production-like run, and `docs/SLO.md` says so until it exists. RPO is a
  deployment property this repository can measure but not set.
