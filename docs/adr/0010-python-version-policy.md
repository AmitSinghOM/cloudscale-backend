# ADR-0010: Python version policy

**Status:** Proposed  **Date:** 2026-09-13
**Enforced by:** *to be* CI matrix over current and next CPython minors
(target 2027-Q1)

## Context
`requires-python = ">=3.12,<3.13"` pins one minor. CPython 3.12 reaches
end-of-life in October 2028. A single-minor pin means the first security
fix that lands only in a newer minor forces an unplanned migration.

## Decision
CI runs the gate on the current minor and the next released minor. The
project supports both and moves its default at least 12 months before the
older minor's end-of-life. `requires-python` widens to match. Any minor
that fails the gate is a blocking bug, not a reason to pin.

## Alternatives rejected
- *Pin one minor forever.* Cheapest today; guaranteed emergency later.
- *Float to any 3.x.* Untested versions are not supported versions.

## Consequences
A second CI job; occasional work absorbing deprecations early instead of
under pressure. Widening to 3.13 is the first step and is on ROADMAP Phase 6.
