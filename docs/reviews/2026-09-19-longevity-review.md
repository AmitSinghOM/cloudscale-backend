# Review 3 — longevity range (v0.5.1 → v0.6.0), 2026-09-19

Four-role review (Staff development engineer / Product engineer / Security engineer / CTO)
of the longevity work that reached `main` through PRs #15–#18 **without a review pass**:
soak harness, event schema evolution (upcasters, `schema_version`, Alembic `0003`),
Python 3.12/3.13 matrix, lock-freshness check, ADRs / charter / contributor and agent
contracts. Range `730356f..ea73293`, 41 files, +2160/−41.

Method: the code-reviewer skill's deterministic pipeline (manifest → 18 risk-ordered bundles
→ per-bundle read against the contract → anchored findings → reflection at the 80 % bar),
a four-role debate per finding keeping only points that change the outcome, and
`code-quality-analyzer 3.1.0` (`--offline --no-project-config`) as the static baseline with
every raw item triaged true/false with a written reason. Prior passes covered the v0.5.x
security hardening and the post-v0.6.0 developer-experience set; this pass deliberately
targeted surfaces neither reached.

## Findings — 8 accepted, all fixed with a test or live verification

| # | Sev | Conf | Finding | Raised / confirmed | Fix | Commit |
|---|---|---|---|---|---|---|
| F1 | **HIGH** | 97 % | Writers stamped `schema_version` from a literal `1` (`EventEnvelope.from_domain_event` default; `command_execution` passed nothing). Following `upcasting.py`'s own bump procedure would have written v1 stamps on v2 payloads; every reader would then apply the 1→2 step to already-current data — silent fold corruption of the source of record on both tiers, invisible to the suite (read path only). **Reproduced**: new test failed `assert 1 == 2` before the fix. | Staff / all | Default derives from `CURRENT_SCHEMA_VERSION[event_type]`; `test_writer_stamps_the_current_schema_version_not_a_literal` | `c8d740a` |
| F1b | MEDIUM | 90 % | Dict-path stores (`PostgresEventStore`, `cqrs.SqliteEventStore`) defaulted an absent stamp to `1` — same skew for legacy callers. | Staff | Shared `_stamp_for()`: `None` → current-for-type; present values stored as given (self-review caught an `or` that would have promoted an invalid 0). | `5b419fc`, `33d8097` |
| F2 | MEDIUM | 88 % | Alembic `0003` downgrade dropped `events.schema_version` unconditionally; after any v>1 row a rollback erased the only record of each row's shape, and re-upgrade relabels all rows v1. | Staff / CTO, Product (rollback is RUNBOOK R1) | `DO $$` guard raises unless every row is v1. Live-PG test inserts a v2 row, asserts refusal + column/revision intact, then v1 → proceeds. Proven to fail against the unguarded migration. | `5b419fc`, `bbda12d` |
| C4 | LOW→MED | 85 % | Soak `evaluate()` dropped samples whose PostgreSQL sampler failed (`pg_error`) from the connection-spread and retention checks — a refused connection (the condition the spread check exists for) made the run look healthier. | Staff / Product, CTO | New criterion `pg_sampler_errors == 0`. | `a8c650e` |
| S2 | MEDIUM | 85 % | A soak shorter than warm-up passed the RSS criteria vacuously ("insufficient") and could print PASS; the 90-second pilot did, and the docstring says a passing report may be committed as evidence. | Product | `MIN_SOAK_SECONDS = 1800` criterion. | `a8c650e` |
| S3 | LOW | 90 % | The soak verdict oracle had zero tests. | Staff | `tests/unit/scripts/test_soak_evaluate.py` (3 tests, synthetic samples). | `a8c650e` |
| B08 | MEDIUM | 85 % | The schema-newer-than-build 503 reached the client but left no operator-visible signal (request log and metrics see only "503"), while RUNBOOK R1 asks operators to recognise the state and redeploy. | Product / Staff | `_LOGGER.error(..., extra={account_id})` before the 503; existing test asserts the record. | `1c70537` |
| D1–D3 | LOW | 90 % | Docs: LONGEVITY labelled a reviewer-enforced rule **Enforced** (charter defines enforced as a failing build); ADR-0009 named only read-side enforcement; README install line said 3.12 only after ADR-0010. | CTO / Staff | Relabelled to Policy with dated plan; ADR-0009 names the write-side and downgrade tests; README fixed. | `7b53a31` |

## Checked and SAFE (no finding) — so false negatives are visible too
- `upcast()`: copies, chains one step at a time, refuses future versions and gaps; registry rejects out-of-range/duplicate steps (tested).
- Legacy rows without the column read as v1 on both tiers (tested).
- Consumer poison path: `UnknownSchemaVersionError` is deterministic → dead-letter, log advances (tested).
- Breaker counts only `transient_errors`; a per-account future event records as a *success* and cannot open the circuit for all accounts.
- `schema.CURRENT_REVISION` bumped to `0003`, so migrations mode fails closed on a stale database.
- Retention `to_regclass` guards match how the adapters create tables; SQLite tier has no PG limiter table to prune.
- API_ERRORS and RUNBOOK already documented the new 503 reason.
- CI `fetch-depth: 0` is exactly what `check_lock_age.sh` requires.
- Every test name and path referenced in LONGEVITY / ADRs / AGENTS / CONTRIBUTING / fixtures README resolves.

## Considered and rejected (< 80 % that it matters)
- `bool` passing `isinstance(raw_version, int)` in `upcast()`.
- Committer-vs-author time in `check_lock_age.sh` under squash merges.
- WAL-pragma `except OperationalError: pass` on both SQLite stores (journal mode does not affect transactional correctness).

## Static analysis (code-quality-analyzer 3.1.0)
Baseline on `main` (f849857): rating **8.1 "Excellent – Comprehensive architecture"**, authoritative,
116/116 files, 66 warnings (22 correctness, 44 maintainability), 0 errors. In-range raw items: 18.

- Correctness (7): **1 true** (C4, above), 6 false positives with reasons — a `BaseException` catch that
  unconditionally re-raises after rollback; the readiness 503 handler (review-2's leak fix); the poison
  dead-letter path (ADR-0009); two WAL-pragma fallbacks; a test helper already `noqa`'d.
- Maintainability (11): 0 defects; all accepted complexity with reasons (harness criteria list and
  orchestrator, consumer failure-class ladder, FastAPI composition root whose 13 parameters are test
  seams, 3 lines over a length limit, one long test).
- **After the fixes**: rating 8.1 → 8.1, authoritative (117/117), deduplicated findings 64 → 64,
  **0 new findings introduced**. The review's value came from reading code against contracts; no
  lexical rule covers F1/F2/S2/B08.
- Analyzer feedback (for cqa-analyzer, not this repo): PY-COR-002 could carry
  `not_when: handler_reraises` for the `except BaseException: …; raise` guard.

## Closing evidence
- `make check` on CPython **3.12.12**: ruff + mypy clean (55 files), **321 passed**, 0 skipped (PostgreSQL-gated tests ran against the live server).
- `make check` on CPython **3.13.12** (throwaway hash-verified venv from `requirements-dev.lock`): **321 passed**.
- `make fresh-tree` (install + gate on a pristine export of exactly what git tracks): **321 passed**, "fresh tree OK".
- Self-review of the fix commits with the same pipeline (13 bundles): one defect found and fixed (`33d8097`); serialized security look: new log carries account id + upcaster message only (no DSN/secrets); downgrade guard exposes a row count only; new `cqrs → cloudscale.domain` import violates no boundary.

## Lessons
1. A mechanism's read path and write path must be tested as one contract. ADR-0009 was "enforced" only from fixtures inward; the first real evolution would have corrupted data the fixtures never see.
2. A harness that can say PASS on a run too short to measure anything will, eventually, be quoted as evidence. Encode the minimum in the oracle, not the docstring.
3. Charter labels are the charter. One "Enforced" that a human enforces is enough to make a reader doubt the rest.
