# ADR-0009: Event schema evolution via upcasters

**Status:** Accepted  **Date:** 2026-09-13
**Enforced by:** `tests/unit/domain/test_upcasting.py` — fixture corpus in
`tests/fixtures/events/` (one file per version ever written), completeness + fold asserted; landed 2026-09-13.
Write side (added 2026-09-19, review 3): `test_writer_stamps_the_current_schema_version_not_a_literal`
asserts a stored row carries `CURRENT_SCHEMA_VERSION[type]`, not a literal — the read path is only
safe if writers stamp what readers upcast *from*. `test_downgrade_0003_refuses_to_erase_non_v1_schema_versions`
asserts the Alembic downgrade fails closed while any row is newer than v1.

## Context
Every event carries `schema_version`, but no code has ever had to read an
old version. Over seven years event shapes WILL change. Without a defined
translation, a 2030 build reading a 2026 event is undefined behaviour on
the source of record.

## Decision
Introduce an upcaster registry in the domain layer:
`(event_type, from_version) -> callable(payload) -> payload` applied at
read time until the payload reaches `CURRENT_SCHEMA_VERSION[event_type]`.
Stored events are never rewritten. A fixture directory holds one sample of
every version ever written; a test folds all of them through the current
projection. Until this lands, event shape changes are restricted to
additive optional fields.

## Alternatives rejected
- *Rewrite stored events on migration.* Destroys the audit property of the
  log and cannot be undone.
- *Versioned projections per schema.* Multiplies consumers; still needs a
  translation for cross-version folds.

## Consequences
One more registry to maintain; the fixture directory becomes a permanent
record. This is the cheapest insurance the project can buy for the data
that outlives the code.
