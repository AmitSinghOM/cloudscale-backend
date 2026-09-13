# AGENTS.md — contract for language-model contributors

You are a contributor, not an oracle. Whatever your generation or
capability, the rules below hold. They exist so that this repository stays
safe to change when nobody who wrote it is present.

## Orientation (read in this order)

1. `README.md` — what the service is and how to run it.
2. `docs/adr/README.md` — the decisions already made and why. Do not
   re-litigate an accepted ADR inside a code change; propose a superseding
   ADR instead.
3. `CONTRIBUTING.md` — the definition of done. It applies to you.
4. `docs/RUNBOOK.md`, `docs/SLO.md`, `docs/THREAT_MODEL.md` — operational
   and security contracts you must keep true.

## Build and verify

```bash
make install-dev
make check           # must be green before any push
```

PostgreSQL-gated tests need `CLOUDSCALE_TEST_PG=postgresql://…`; without it
they skip locally and CI will still run them. Do not report "tests pass"
from a run in which they skipped.

## Invariants you may never weaken

- Domain and application layers import no frameworks
  (`tests/architecture/`).
- A retried `command_id` never double-applies.
- Events are append-only; the event log is never pruned or rewritten.
- Exactly one consumer per name drains the projection.
- Production refuses to start against an unmigrated schema.
- Authentication and authorization are default-deny.
- No production code path calls a language model.

If a task seems to require weakening one of these, stop and say so. The
task is wrong, not the invariant.

## Evidence rules

- A performance or availability number exists only if a harness report
  bound to the commit exists under `evidence/<sha>/`.
- Never commit failing evidence. Never adjust a threshold to make evidence
  pass. State shortfalls with attribution.
- Only 10-second (or longer) gate runs count; shorter smokes are for
  checking the harness, not the service.

## Actions that require a human to approve the specific action

Deleting or truncating data · dropping a database that you did not create
in this session · rotating production secrets · changing authorization
semantics · tagging or publishing a release · force-pushing or rewriting
shared history · disabling a CI check.

Ask, state the exact command, and wait. Confidence is not authorization.

## Working style that keeps this repository healthy

- One concern per branch; push to a side branch; open a PR; merge on green.
- Prefer the smallest change that makes the failing test pass, then stop.
- Write the commit body for a reader who has only the diff and this file.
- When you learn something the docs got wrong, fix the docs in the same PR.
- When you are unsure whether a behaviour is a constraint or an accident,
  look for an ADR. If there is none, write one and ask.
