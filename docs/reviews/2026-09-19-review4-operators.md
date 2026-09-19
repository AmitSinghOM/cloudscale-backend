# Review 4 — operator tooling, logging, supply chain, redrive (post-v0.6.0), 2026-09-19

Four-role review (Staff development engineer / Product engineer / Security engineer / CTO)
of the surfaces the three earlier passes never reached: operator tooling (`scripts/dlq.py`,
RUNBOOK R1), what the logs emit and omit, GitHub Actions supply chain (threat-model B7 covered
dependencies and the image but not the actions), the auth edge cases outside the review-1/2
tests, the secrets-handling controls added in review 2, and the RUNBOOK's "redrive is
exactly-once" claim.

Method: the code-reviewer skill's deterministic pipeline (manifest → risk-ordered bundles →
read against the contract → anchored findings → reflection at the 80 % bar); the
security-reviewer skill's stack-aware audit (`pip-audit --strict` on both locks,
`ruff --select S`, secrets sweep) run as a separate serialized pass; a four-role debate per
finding; every suspect **verified empirically before being asserted** (the documented
operator command was run, the race was raced); and `code-quality-analyzer 3.1.0`
(`--offline --no-project-config`) before and after, with every raw item triaged in writing.
Baseline: the post-review-3 report at `e922ad7`.

## Findings — 8 accepted, all fixed with a test or live verification

| # | Sev | Conf | Finding | Raised / confirmed | Fix | Commit |
|---|---|---|---|---|---|---|
| F1 | **HIGH** | 97 % | `scripts/dlq.py` only opened SQLite files. RUNBOOK R1 tells operators to run `dlq.py <projection-db-or-dsn> redrive`; against a PostgreSQL DSN it returned `database not found`, exit 2. On the production tier there was **no supported way to inspect or redrive dead letters** — the recovery for the poison-event freeze and the ADR-0009 schema-newer-than-build freeze — pushing an operator toward direct SQL on `dead_letters`, which is exactly threat-model B6's risk. `PostgresProjectionStore.redrive`/`redrive_all` existed and nothing called them. **Reproduced** by running R1's own first step. | Staff / Security, CTO; Product (the runbook promised a DSN) | DSN dispatch to `PostgresProjectionStore` behind a small `Protocol` both stores satisfy. Live-PG test parks a letter, runs `list --json`, `show`, `redrive --all` through the CLI, asserts the balance applied; fails on the old script. | `4eb4b25` |
| F1b | MEDIUM | 88 % | Self-review of F1: opening the store from the CLI ran the adapter's default `auto` schema mode, i.e. `CREATE … IF NOT EXISTS` under the operator's credentials — DDL on production from a support tool, and a mistyped DSN would silently grow a full schema in the wrong database (contradicts ADR-0007). | Staff / CTO (ADR), Security (least privilege) | The tool forces `migrations` semantics unless `CLOUDSCALE_PG_SCHEMA` is set explicitly; an unmigrated database is exit 2. Test proves an empty database is refused and stays empty. | `ed6f91d` |
| F2 | MEDIUM | 85 % | `PostgresProjectionStore.redrive` read the letter, applied it, then deleted it, so two operators (or two `redrive --all`) racing on one event **both applied it** — a double-applied deposit — contradicting RUNBOOK R1 step 4 "redrive is exactly-once". | Staff / CTO; Product (concurrent redrives during an incident are the normal case) | `DELETE … RETURNING payload` in the same transaction as the balance mutation; the loser blocks on the row lock and reports `NOT_FOUND`. Race test (two stores, five rounds) showed both `APPLIED` on the old code, passes on the new. | `350900f` |
| F3 | MEDIUM | 90 % | Insufficient security logging (OWASP A09): every 401 discarded its reason and every 403 was silent beyond the access log's status, so a **revoked admin token still in use left no trace anywhere**, and a 401 spike from a rotation gone wrong was indistinguishable from an attack. | Security / Staff, CTO; Product | `auth.rejected` at WARNING with a controlled vocabulary (parser exception class, `lifetime_exceeded`, `missing_subject`, `admin_without_jti`, `revoked` with subject + jti, `missing_bearer`) and `authz.denied` with subject + account. Client still gets the single fixed message (no oracle); volume bounded by the pre-auth rate limit. Test asserts the operator record and that the jti never reaches the body. | `3ccd90f` |
| F4 | MEDIUM | 88 % | The harness secret in `scripts/http_gate_run.py` is as public as the compose one but was not in `KNOWN_DEVELOPMENT_SECRETS`, so review 2's production-mode startup warning could never fire for it — an allowlist-shaped control is only as good as the list's completeness. | Security / Staff | Registered; `tests/architecture/test_known_development_secrets.py` scans every shipped file (not `tests/`) for secret literals and asserts the set is complete with no stale entries. Proven to fail against the pre-fix `settings.py`. | `3d5bd44` |
| F5 | LOW | 85 % | Seven of eight GitHub Actions pinned to moving major tags; only Trivy was SHA-pinned. Blast radius already small (`contents: read`, no secrets, image never pushed) so this is integrity — a retagged action could turn a red gate green — not exfiltration. | Security / CTO (ADR-0008 exact-pin rule); Product: no user impact | All eight pinned to the commits the tags resolve to today (resolved read-only via the API; no behaviour change), version comments kept for Dependabot; threat model B7 records the threat. | `7e9524c` |
| F6 | LOW | 90 % | Doc drift in my own F3 fix: the docstring listed reason values that did not exist (`expired`, `issuer_mismatch`). | Staff (self-review) | Docstring names the real vocabulary. | `ed6f91d` |
| F7 | LOW | 85 % | `check_fresh_tree.sh` says it leaves the export in place *on failure* but never removed it on success either — a full venv accumulated under the scratch dir per run. | Staff / Product | Removed on success, kept on failure. | `464086a` |

## Checked and SAFE (no finding) — so false negatives are visible too
- Auth: algorithm-confusion refusal at config time; HMAC rejected in JWKS mode; unknown-key
  rejection; rotation via `kid`; lifetime cap; required `iat`; revoked `jti`;
  relaxed-queries-but-never-commands; pre-auth flood budget — all already locked by tests;
  `jwt.decode` runs with an explicit algorithm allowlist and required claims.
- Logs: the access log records method, route template, status, latency, subject and client
  address — no amounts, headers or tokens; the audit log records identifiers and outcomes only.
  The new rejection logs carry class names, a subject, a jti and a route path — no token material.
- `pip-audit --strict` on `requirements.lock` and `requirements-dev.lock`: no known vulnerabilities.
- `ruff --select S`, inline suppressions ignored: 3 raw, 0 true (two `random` uses shaping
  load in the soak harness; one constant 401 message string).
- CI token permissions already `contents: read`; no repository secrets consumed; image is built
  and scanned, never pushed.
- The SQLite `redrive` has the same read-then-apply shape but a process-local lock; its
  cross-process race is recorded here, not fixed — SQLite is the development tier.

## Considered and rejected (< 80 % that it matters)
- Rate-limiting the new rejection log separately: the pre-auth client rate limit already
  bounds it and runs first.
- Making the redrive `DELETE … RETURNING` pattern shared with SQLite: different locking model;
  not worth the abstraction for the dev tier.

## Static analysis (code-quality-analyzer 3.1.0)
Before (`e922ad7`) → after (`464086a`): rating **8.1 → 8.1** "Excellent", authoritative
119/119 files, **66 → 66** deduplicated findings, zero new, zero removed. One finding was
introduced mid-pass and fixed before the close: `PY-MAINT-003` on `authenticate` (61 lines
after the inline F3 logging) — a true positive by the repo's own rule, resolved by folding
log-and-401 into `_reject()` (`4d4feb4`). None of the eight accepted defects was visible to
a lexical rule; the analyzer's role here is the upgraded-not-degraded proof, not discovery.

## Closing evidence
- `make check` on CPython 3.12.12: ruff clean, mypy clean (55 files), **327 passed**, 0 skipped.
- `make check` on CPython 3.13.12 (hash-verified venv from `requirements-dev.lock`): **327 passed**.
- `make fresh-tree` (install + gate on a pristine export of exactly what git tracks): **327 passed**.
- Code-reviewer skill pass over the pass-4 fix commits themselves: found F1b and F6.
- 327 = 321 after review 3 + six tests added here (DLQ round-trip, unmigrated refusal, SQLite
  not-found, known-secrets completeness, auth rejection record, redrive race).

## Lessons
- Run the documented operator command against the production tier; a tool that only opens
  the dev store makes a runbook unexecutable exactly where it matters (F1).
- An allowlist-shaped warning needs a test that keeps the list complete against every shipped
  literal, or it silently stops covering new files (F4).
- "Exactly-once" for an operator action is a claim about concurrent operators; race it (F2).
- Self-review of the fixes found two of eight findings (F1b, F6); the reviewer's own commits
  are a bundle like any other.
