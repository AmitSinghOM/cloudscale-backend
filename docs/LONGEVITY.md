# Longevity Charter — keeping CloudScale safe to change through 2033

Software does not die of age. It dies when the cost of a *safe* change
exceeds the value of the change. Everything in this document lowers that
cost for a maintainer who was not present when the decisions were made —
a person, a future language model, or something stronger. The mechanism is
the same for all three: **the reasoning is written down, the invariants are
executable, and the gates do not care who the author is.**

Each item below is marked **enforced** (a machine fails the build if it is
violated) or **policy** (a human must remember). Every policy item has a
dated plan to become enforced or a reason it cannot.

## 1. Invariants live in tests, not in heads

- **Enforced.** Architecture tests (`tests/architecture/`) fail the build if
  the domain or application layer imports a framework. The hexagonal
  boundary is therefore a fact, not a convention.
- **Enforced.** Every correctness property that has ever mattered has a
  test: idempotency under retry, exactly-once projection, outbox ordering,
  autocommit visibility, lease exclusivity and failover, schema drift
  between `auto` and `migrations`, error-text leakage.
- **Enforced.** Event schema evolution (ADR-0009): every event row carries
  `schema_version`; `cloudscale.domain.upcasting` translates at read time;
  `tests/fixtures/events/` holds one row per version ever written and the
  suite fails if the corpus is incomplete or any fixture fails to fold.

## 2. Decisions are recorded with their *why*

- **Enforced.** `docs/adr/` holds one Architecture Decision Record per
  load-bearing decision, numbered, immutable once accepted; superseding is
  a new ADR that links back. The initial set (ADR-0001..0008) reconstructs
  the decisions already in the code so a future maintainer can distinguish
  a constraint from an accident.
- **Policy** (relabelled 2026-09-19 — a reviewer, not a machine, rejects;
  by this charter's own definition that is not *enforced*). A pull request
  that changes a public contract (HTTP route, event shape, table,
  environment variable) must reference an ADR or add one; `CONTRIBUTING.md`
  says so. Partial machine coverage exists: `docs/openapi.json`,
  `docs/CONFIGURATION.md` and the fixture corpus each fail the build when
  the route, variable or event shape changes without the matching artefact.
  Plan: 2027-Q1, a CI check that a PR touching those surfaces also touches
  `docs/adr/`.

## 3. Builds are reproducible for the whole horizon

- **Enforced.** Exact-pinned dependencies with hash-verified installs;
  base image receives security updates at build time; `pip-audit` and Trivy
  fail CI on fixable findings.
- **Enforced.** Python version policy (ADR-0010): the CI gate runs on the
  current and next CPython minors (3.12 and 3.13 today); `requires-python`
  allows both; the project moves off a minor at least 12 months before its
  end-of-life (3.12 EOL 2028-10).
- **Enforced.** Quarterly dependency refresh: `scripts/check_lock_age.sh`
  runs in CI and fails the build when either lock file has gone more than
  120 days without regeneration (warns at 90). Regenerate both locks with
  the recorded `uv pip compile` command, run the full gate and a 10-second
  gate run on both tiers, commit the diff with the report. Dependabot
  proposes; the staleness check makes the quarter close.

## 4. Data outlives code

- **Enforced.** The event log is the only source of record; every other
  table is derived and rebuildable (RUNBOOK R7 lists which). Alembic
  revisions are the only way schema changes reach production
  (`CLOUDSCALE_PG_SCHEMA=migrations` refuses anything else).
- **Enforced.** Events are stored as JSON with explicit `schema_version`,
  not pickled objects — readable by any language in any decade.
- **Enforced.** Upcasters (ADR-0009): registry of one-step translations
  chained to the current shape; stored events are never rewritten; a
  version newer than the build dead-letters and the command path answers
  503, so a rolled-back deploy freezes accounts rather than corrupting them.

## 5. Operations are rehearsed, not documented

- **Enforced.** Soak harness (`scripts/soak_run.py`) with criteria fixed
  in code; gate harness with per-commit evidence.
- **Policy.** Annual restore drill: `pg_dump` a production-like database,
  restore to a fresh instance, run `migrate current`, start in
  `migrations` mode, rebuild the projection from the log, and compare
  balances. Record the time taken in `evidence/<sha>/restore-drill/`.
- **Policy.** Quarterly failover drill in the pilot environment: SIGKILL
  the leader consumer; confirm the standby leads within 5 s.

## 6. Knowledge survives people

- **Enforced.** `CONTRIBUTING.md` defines done: gate green, evidence rule,
  ADR for contract changes, changelog entry. `CHANGELOG.md` is kept by
  release. `CODEOWNERS` names who must review what.
- **Policy.** Bus factor ≥ 2 before any external SLA: a second maintainer
  has merged at least one non-trivial change and performed one restore
  drill.

## 7. AI contributors are governed by the same gates, plus a contract

Language models — whatever generation — are treated as contributors, not
oracles. `AGENTS.md` is the machine-readable contract: how to build and
test, which invariants may never be weakened, which actions require a
human. The principles:

- **Correctness never depends on a model.** No production path calls an
  LLM. Models write code; tests decide whether it ships.
- **Same gates, no exceptions.** An agent's pull request passes the
  identical CI a human's does. There is no "trusted agent" bypass and there
  never will be.
- **Evidence, not assertion.** An agent claiming a performance number
  must attach the harness report bound to the commit, exactly as a human
  must. Failing evidence is never committed.
- **Small, reversible changes.** Agents work on branches, one concern per
  PR, merged only on green. A stronger future model does not change this;
  it only makes the loop faster.
- **Humans own the irreversible.** Deleting data, rotating production
  secrets, changing authorization semantics, tagging a release: a person
  approves, whatever the model's confidence.

## Review of this charter

Re-read at every tagged release. Any **policy** item still unenforced one
year after its target date is either enforced, given a new dated plan with
a written reason, or removed. Aspirations that nobody enforces are deleted,
not carried.

*Adopted 2026-09-13 at v0.5.1; reviewed at v0.6.0 the same day (four policy items became enforced).*
