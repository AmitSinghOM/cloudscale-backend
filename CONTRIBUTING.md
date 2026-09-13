# Contributing

This file is the definition of *done*. It applies to every contributor —
people and language-model agents alike (agents: also read `AGENTS.md`).

## Before you start

- Read `README.md`, then the ADRs in `docs/adr/` that touch your area. If
  your change contradicts an accepted ADR, write a superseding ADR first
  and get it reviewed; do not "just change it".
- One concern per pull request. Small and reversible beats large and clever.

## Local loop

```bash
make install-dev          # hash-verified, from requirements-dev.lock
make check                # ruff format-check · ruff (incl. S) · mypy · pytest
```

`make check` must be green before you push. PostgreSQL-gated tests skip
without `CLOUDSCALE_TEST_PG`; CI runs them against a real PostgreSQL 17 and
fails if they skipped, so a green local run without PostgreSQL is not proof.

## Definition of done

A pull request merges when **all** of the following hold:

1. CI is green: gate, dependency audit, image build + scan.
2. New behaviour has a test that fails without the change. Bug fixes add a
   regression test named after the bug.
3. Public contract changes (HTTP route/response, event shape, table,
   environment variable, CLI flag, exit code) reference an ADR or add one.
4. Operational behaviour changes update `docs/RUNBOOK.md`; SLI/metric
   changes update `docs/SLO.md`; security-relevant changes update
   `docs/THREAT_MODEL.md`.
5. `CHANGELOG.md` has an entry under *Unreleased*.
6. Performance claims come with harness evidence bound to the commit
   (`scripts/http_gate_run.py`, `scripts/soak_run.py`) under
   `evidence/<sha>/`. **Failing evidence is never committed.** Shortfalls
   are stated with their attribution, in prose, not hidden.
7. The commit message says *why* (Conventional Commits; body explains the
   trade, not the diff).

## Things that are never acceptable

- Weakening a test to make it pass.
- Adding a dependency with a floating version, or bypassing
  `--require-hashes`.
- Catching an exception to hide it. Catch to translate, log, or recover.
- Changing authorization semantics, deleting data, rotating secrets, or
  tagging a release without a human approving that specific action.
- Claiming a number that is not in a committed evidence file.

## Releases

Tag `vX.Y.Z` on the merge commit; bump `pyproject.toml`; move the
*Unreleased* changelog section under the version; update the README status
line. Patch releases change failure behaviour or fix bugs; minor releases
add capability; a major release changes a public contract incompatibly and
requires a superseding ADR.

## Security

Do not open public issues for vulnerabilities. See `SECURITY.md`.
