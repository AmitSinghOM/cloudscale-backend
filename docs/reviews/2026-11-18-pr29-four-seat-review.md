# Review 7 — ADR-0018 / 0020 / 0021 change set (PR #29, post-v0.10.0), 2026-11-18

Four-seat review (Staff development engineer / Product engineer / Security engineer / CTO)
of the slice landed as `445bf7b`, the CI/dependency fix `4412309` and the CQA refactor
`336f731` on `fix/cloudscale-all-debit-authorization`: all-debit posting authorization
(ADR-0018), independent restore integrity (ADR-0020), self-registration containment
(ADR-0021), the Proposed provisioning policy (ADR-0019), the PostgreSQL 17 client in
the gate jobs, and the advisory bumps (pyjwt, certifi, OpenTelemetry). Surfaces earlier
passes never reached: the authorization boundary of a multi-debit set, fingerprint
comparison of system-of-record tables across dump/restore/rebuild, a route turned into
an unconditional 403, and the repo's own release policy against a contract break.

Method: **four independent agents, no shared context, read-only**, one per seat, each
given the same brief (`docs/reviews/` convention: verbatim `existing_code` per finding,
> 80 % confidence, dismissed candidates recorded), over the deterministic manifest of the
code-reviewer skill (30 files, 12 risk-ordered bundles, 0 skipped). Adjudication by the
author's session; every accepted finding fixed in this commit. `code-quality-analyzer
3.5.0` before (`081e2ba`, via a worktree) and after. Gate on this host with live
PostgreSQL 16.2 (`pgserver`) and a matching `pg_dump`: 592 passed, 0 skipped.

Seat outputs: Staff 2 findings, Product 3, Security 0 (with 2 pre-existing follow-ups),
CTO 2. The two MEDIUMs about the version number are the same finding from two seats.

## Findings — 6 accepted, 5 fixed here, 1 left to the release owner

| # | Sev | Conf | Finding | Seat | Fix |
|---|---|---|---|---|---|
| F1 | MEDIUM | 92 % | `docs/API_ERRORS.md` postings section still described ADR-0013 ("the anchor: it is authorized"), had no 403 row, and its replay row promised success unconditionally — while `commands.py` now returns 403 for any unauthorized debit and on replays that lost access. The revert section already had the right row shape; postings was missed. | Product | Section header names ADR-0018; the prose states the every-debit rule; a 403 row modelled on the revert row (generic body, nothing persisted, same `command_id` retryable); the replay row is conditional on current access. `test_api_errors_docs` keeps the table and code in step. |
| F2 | MEDIUM | 90 % | `SECURITY.md` ("Verified by tests…") still listed "ownership registered via `POST /v1/accounts`" as a way to obtain access and claimed an audit log of "every command outcome and registration". After ADR-0021 the route is an unconditional 403 and `audit_registration` is unreachable from HTTP. | Product | Authorization bullet rewritten: existing bindings only, self-registration disabled (ADR-0021), provisioning proposed (ADR-0019), every debited account for posting sets (ADR-0018). Audit bullet: "every command outcome". |
| F3 | MEDIUM | 86 % | **Release policy.** `CONTRIBUTING.md` "Releases": "a major release changes a public contract incompatibly and requires a superseding ADR"; `docs/adr/README.md` counts "HTTP route or response" as public contract. This slice removes the 201/200/409/400/501 responses of `POST /v1/accounts` and turns accepted multi-debit sets into 403s, each with a superseding ADR. No pre-1.0 exemption is written anywhere. A `v0.11.0` MINOR tag contradicts the repo's own rule, and the entries sat under *Changed* / *Fixed*. | Product + CTO | **Partly here:** both breaks moved to a `### Removed (BREAKING …)` heading naming the removed responses and the client action. **Left to the release owner** (a human decision): cut `1.0.0` as the policy stands, or amend `CONTRIBUTING.md` with an explicit pre-1.0 rule in the release PR and tag `v0.11.0`. |
| F4 | MEDIUM | 82 % | **Release coupling.** ADR-0018 itself rejects coupling the fix to the registration redesign ("must not delay closing the multi-debit authorization gap"), yet ADR-0018, the advisory bumps and ADR-0021's gated cutover ("no deployment … authorized" until callers are inventoried) ship in one commit and one planned tag. If that tag is the only artifact, the theft fix and the dependency fixes wait on the onboarding cutover. | CTO | **Recorded, not changed here.** The three are already one commit (`445bf7b`); splitting means reverting `routes/accounts.py` + ADR-0021 + its tests onto a patch branch. Offered to the release owner as an option alongside F3; the ADR-0021 Consequences and RUNBOOK "Registration containment" already make the deploy gate explicit, so a single artifact with that gate is defensible if chosen knowingly. |
| F5 | LOW | 85 % | `_FINGERPRINT_SELECT` precomputed `ORDER BY 1` for every classified table under a comment claiming every table has its PK first. `transfer_legs` (DERIVED) has a composite PK, so its digest would be nondeterministic if anyone fingerprinted it trusting the comment. Only `SYSTEM_OF_RECORD` is fingerprinted today, so nothing fails yet. | Staff | Dict restricted to `schema.SYSTEM_OF_RECORD`; the comment names the four single-column keys and says why derived tables must not be added as-is. Any other name is a `KeyError`. |
| F6 | LOW | 82 % | The gate's no-skip step omitted `tests/unit/scripts/test_restore_integrity.py`, the first test ADR-0020 names as its enforcement; its PostgreSQL parametrization skips whenever `CLOUDSCALE_TEST_PG` is absent, so a future env/fixture change could let the PG half of ADR-0020 skip silently. | Staff | Added to both no-skip steps (gate and restore-drill job). |

## Candidates considered and dismissed (with the evidence that cleared them)

| Candidate | Seat | Why declined |
|---|---|---|
| A 403 on a replay could leave a stored result or touch `command_results` | Staff, Security | `_authorize_every_debit` runs after shape validation and **before** `ctx.executor.execute`; no idempotency middleware exists; `test_postings_require_every_debit_before_persisting` asserts `stored_result`/`legs_of` unchanged after a denied replay (secondary debit at index 0, 1 and 2). |
| Leg tricks: duplicate account id, anchor twice, `Debit` casing, reordering | Security | `Post.__post_init__` rejects duplicates (`duplicate_account`); `direction` is `Literal["debit","credit"]` (422 first); the loop walks every leg; no layer normalizes account ids. |
| Transfer / Revert / hold routes outside ADR-0018's wording | Security | Transfer debits only the authorized path account; Revert already authorizes every account it debits from the log; holds act on the path account's own hold. |
| Existence oracle on the new 403s | Security, Staff | Anchor 403 → shape 400 (body-only) → secondary 403, same detail, before any fold/balance read (test patches `CommandExecutor`); `owner_of` runs one lookup for unregistered and foreign accounts alike. |
| `POST /v1/accounts` not truly unconditional / OpenAPI still advertising 201 | Security, Product | `register_account` raises after auth + body validation, no registry/event/result access, admin included; `openapi.json` lists 401/403/422/429/503 only; `test_openapi_contract` asserts 200/201/400/409/501 absent and the document current. |
| Fingerprint false failure (row order, bytes/memoryview, tz, `published`, SQLite affinity) | Staff | All four SOR tables have a unique first column; bytes → hex, datetimes → UTC ISO; `published` is the only column `truncate_derived` rewrites and is excluded; SQLite copy is a page copy of the same file; nothing between the restored and rebuilt snapshots writes SOR tables. |
| Fingerprint false pass | Staff | Newline-separated compact JSON (escapes newlines/quotes), column names hashed, row count compared, bytes/datetimes tagged; the 9 SQLite corruption cases all fail the drill. |
| `_FINGERPRINT_SELECT` f-string flagged by CQA (PY-COR-007) | Staff, Security | Built once at import over `frozenset` literals of table names; no CLI or user value reaches it; an unknown name is a `KeyError`, never SQL. Recorded as a safe boundary pattern, not fixed. |
| pyjwt 2.15.1 weakens or breaks verification | Security | Release notes read; 2.15.1 probed through `auth.py`: alg=none, HS512 under HS256 allowlist, deep nesting, non-int exp/iat/nbf, int `sub`/`jti`, unexpected `aud` all rejected as `InvalidTokenError` → fixed 401; HMAC, JWKS and auth tests pass. Lock hashes match PyPI; the new transitive `opentelemetry-exporter-otlp-common` is the `opentelemetry` org's. |
| Quickstart / CI compose smoke broken by ADR-0021 | Product | `scripts/dev_token.py` mints `accounts:admin`; README `make token`, `compose.yaml`, `examples/python_client.py` and the CI smoke never called `POST /v1/accounts`. The provisioning gap is stated in API_ERRORS and RUNBOOK with the ADR-0019 pointer. |
| RUNBOOK / SLO restore-drill text vs code | Product, CTO | Every flag (`--dsn`, `--seeded`, `--dump`, `--source-dsn`, `--sqlite-dir`, `--pg-bin`, `--report`), exit 3, the `backup_*`/`rebuild_*` criterion names and the three `*_integrity` duration keys exist in `restore_drill.py`. |
| ADR status transitions, Enforced-by tests | CTO | README convention followed (Supersedes / Superseded by); every test named by ADR-0018/0020/0021 opened and exists; ADR-0017 stays Proposed with the incompatibility note, which the "immutable once accepted" rule does not cover. |
| `build_router` CQA finding | Author | Pre-existing (212 → 213 lines); the new check is `_authorize_every_debit`. |

## Pre-existing, outside this change — follow-ups, not filed

- `JwksVerifier`: an unknown or missing `kid` raises `jwt.PyJWKClientError`, a `PyJWTError` but not an `InvalidTokenError`, so `authenticate`'s `except` does not catch it and the response is likely a 500, not the fixed 401. Same on 2.13; no unknown-`kid` test exists. Fix: catch `jwt.PyJWTError` in `JwksVerifier.verify`, with a test. (Security)
- `audit_registration` (`observability.py`) and `AccountRegistry.register` are unreachable from HTTP; `register` is still used by the drill seed and tests. Cleanup candidate once ADR-0019 decides the provisioning path. (Product, Security)
- ADR-0013 "Enforced by" names `tests/architecture/test_api_errors_doc.py` and `tests/unit/entrypoints/test_openapi_contract.py`; the real files are `tests/architecture/test_api_errors_docs.py` and `tests/architecture/test_openapi_contract.py`. (CTO)
- CI fetches the PGDG apt key over HTTPS with no fingerprint check, and the compose smoke's `pip install pyjwt==2.15.1` is not hash-pinned; both pre-date this change. (Security)
- `docs/LONGEVITY.md` §4 still says "every other table is derived" although ADR-0016's `SYSTEM_OF_RECORD` has four tables. (CTO)

## CQA (`code-quality-analyzer 3.5.0`)

`081e2ba` → `445bf7b`: 8.2 → 8.2, 71 → 74 findings (four introduced). After `336f731`
and this commit: 71 → 72 — the one remaining delta is the pre-existing `build_router`
length finding relabelled 212 → 213; the `_FINGERPRINT_SELECT` f-string is recorded above
as a safe boundary pattern.

## Closing evidence

`make check` with `CLOUDSCALE_TEST_PG` live (PostgreSQL 16.2 via `pgserver`, matching
`pg_dump`): 592 passed, 0 skipped, ruff format/check and mypy clean. CI on PR #29 at
`c07b1c6`: five jobs green, gate `pg_dump` 17.12, 592 passed with no skips.
