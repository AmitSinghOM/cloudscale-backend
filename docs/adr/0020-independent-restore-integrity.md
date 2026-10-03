# ADR-0020: Independent restore integrity and restored command replay

**Status:** Accepted
**Date:** 2026-10-03
**Enforced by:** `tests/unit/scripts/test_restore_integrity.py`,
`tests/unit/scripts/test_restore_drill.py`,
`tests/unit/adapters/test_postgres_restore_drill.py`, and
`tests/unit/entrypoints/test_restored_replay.py`.

## Context

ADR-0016 classifies ownership, command outcomes, envelopes, and financial events
as system of record. Its original drill reads its comparison baseline *after*
restore. A financial fold can therefore agree while ownership or rejected
command outcomes have disappeared. Counts alone also miss content substitution.

This supplements ADR-0016's comparisons, without changing classification,
retention, provisioning authority, consumer fencing, or financial semantics.
The independent restore/replay test plan was approved on 2026-10-03.

## Decision

In seeded mode, capture canonical content fingerprints of every table in
`schema.SYSTEM_OF_RECORD` from the quiescent source **before dumping it**.
Compare them with the restored copy before adapters or rebuild can change it.
Also compare that restored baseline against the post-rebuild content in both
seeded and existing-dump modes. Keep every original fold/read-model comparison.

A fingerprint contains the row count and SHA-256 of column names and ordered,
length-unambiguous JSON rows. Include every column except `events.published`,
which ADR-0016 explicitly classifies as derived. Normalize binary values and
UTC timestamps; do not reinterpret serialized command outcomes. PostgreSQL
uses a repeatable-read read-only transaction and server-side cursors; SQLite
uses one read transaction. No table-wide row materialization is required.
These are same-tier comparisons, not a cross-database migration format.

Criteria are named `backup_<table>_preserved` and
`rebuild_<table>_preserved`. Any exercised mismatch fails the drill (exit 3).
Reports contain only counts and equality verdicts, never rows, identities,
request hashes, or table digests. Empty comparisons are marked unexercised.
A successful corruption regression means a correctly **failed drill**, not
positive recovery evidence.

An existing dump alone cannot prove what was lost during backup. Its four
backup criteria are explicitly `exercised: false`; a current live source is
not the historical snapshot. Exit 0 in this mode proves the exercised rebuild
criteria, **not independent backup integrity**. A future production backup
integration must supply a trusted manifest bound to the exact dump checksum,
schema, and consistent source snapshot. This change does not add a manifest
producer/consumer or invent a trusted manifest by reading the restored copy.

Restored HTTP tests retain original request bodies, identities, and responses.
On both tiers they require stable accepted/rejected replay, changed-payload
conflict, retained registry ownership, outsider denial, and authorization of
all debit legs before replay lookup. PostgreSQL additionally exercises concurrent
restored retries. No rejected request is reconstructed from financial events.

## Alternatives rejected

- Counts alone: same-count owner, request-hash, outcome, or event substitutions
  still pass.
- Folding the restored log alone: cannot establish ownership or rejected results.
- Storing raw row dumps in reports: unnecessary identity/financial exposure.
- Comparing a live source to an older dump: reports legitimate later writes as
  corruption and falsely attributes them to the backup snapshot.
- Changing retention or registration to make replay tests pass: separate policy
  decisions; backup preservation does not establish indefinite retry safety.

## Consequences

The drill adds three authoritative scans in seeded mode and two in dump mode,
with constant application-side row memory on PostgreSQL. This is additional
verification work, not a throughput or recovery-SLO claim. Seeded success does
not establish production-scale RTO, historical issuance, or backup authenticity.
No deployment, existing database mutation, or production dump is required by
the regression suite.
