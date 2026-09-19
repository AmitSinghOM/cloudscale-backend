# API errors and how to handle them

Every non-2xx response is one of the cases below. The table's *client
action* column is the contract: a client that follows it is safe under
retries, concurrency and rolling deploys. Checked by
`tests/architecture/test_api_errors_docs.py` — every domain error code and
command outcome in code must appear here.

## Command endpoint (`POST /v1/accounts/{account_id}/commands`)

The response body for 2xx/409/422/400 is a `CommandResult`: `outcome`,
`error_code`, `expected_version`, `current_version`, `committed_version`,
and `postings` — one `{account_id, event_id, committed_version}` per stream
the command wrote (one entry for a deposit or withdrawal; empty on rejection).

| Status | `outcome` / `error_code` | Meaning | Client action |
|---|---|---|---|
| 201 | `accepted` | Event appended; `committed_version` is the stream's new version. | Continue. To read your write, poll the balance until `version ≥ committed_version`. |
| 201 | `accepted` (replay) | Same `command_id` and identical request seen before; the stored response is returned byte-for-byte (status included), nothing re-executed. | Treat as success. This is the idempotent retry path — **always retry with the same `command_id`.** |
| 409 | `version_conflict` | `expected_version` ≠ the stream's `current_version`: another writer got there first. | Re-read `current_version` from the body, re-run your business decision, resubmit with a **new** `command_id`. |
| 409 | `command_id_conflict` | Same `command_id` reused with a *different* request body. | Bug in the client: a `command_id` identifies one intent. Mint a new id for a new intent. |
| 422 | `insufficient_funds` | Withdrawal exceeds the balance at `expected_version`. | Business rejection. Do not retry unchanged. |
| 400 | `domain_rejected` + one of the domain codes below | Request violates a domain invariant. | Fix the request. Never retry unchanged. |

## Transfer endpoint (`POST /v1/accounts/{account_id}/transfers`)

Moves `amount` from the path account (the **source**) to `target_account_id`
as two postings committed in one transaction (ADR-0011). The caller must be
authorized for the source only; the target may be any account. The body is
the same `CommandResult`; `account_id`, `committed_version` and `event_id`
describe the source, and `postings` carries both legs. The target posting's
`committed_version` is `null` unless the caller may also read that account.

| Status | `outcome` / `error_code` | Meaning | Client action |
|---|---|---|---|
| 201 | `accepted` | Debit appended to the source, credit to the target; both in `postings`. | Continue. The read model is eventual **per account**: poll each balance to its posting's `committed_version` (when visible) before relying on it. |
| 201 | `accepted` (replay) | Same `command_id` and identical request seen before; stored response returned byte-for-byte. | Treat as success; always retry with the same `command_id`. |
| 409 | `version_conflict` | `expected_version` ≠ the **source** stream's `current_version`. | Re-read the source's `current_version`, re-decide, resubmit with a new `command_id`. |
| 409 | `command_id_conflict` | Same `command_id` reused with a different request body. | Client bug; mint a new id. |
| 422 | `insufficient_funds` | Amount exceeds the source balance at `expected_version`. | Business rejection. Do not retry unchanged. |
| 400 | `same_account` | Source and target are the same account. | Fix the request. |
| 400 | `domain_rejected` + a domain code | E.g. `amount_out_of_range` when the credit would overflow the target. | Fix the request. Never retry unchanged. |

## `POST /v1/accounts/{account_id}/postings` (ADR-0013)

An N-leg balanced posting set: 2 to 16 legs (`MAX_LEGS`), each
`{account_id, amount, direction: debit|credit}`, committed in one
transaction. The path account is the **anchor**: it is authorized, it is the
only stream whose `expected_version` the caller supplies, and it must be one
of the debited legs. Debits must equal credits. Success returns the same
`CommandResult` shape as a transfer with one entry per leg in `postings`;
`committed_version` is `null` for legs on accounts the caller may not read.
`/transfers` is the two-leg convenience and is unchanged.

| Status | `outcome` / `error_code` | Meaning | Client action |
|---|---|---|---|
| 201 | `accepted` | Every leg appended, one per stream, in `postings`. | Continue; poll each visible balance to its `committed_version`. |
| 201 | `accepted` (replay) | Same `command_id`, identical request (including leg order). | Treat as success. |
| 409 | `version_conflict` | `expected_version` ≠ the **anchor** stream's version. | Re-read, re-decide, resubmit with a new `command_id`. |
| 409 | `command_id_conflict` | Same `command_id`, different request (a reordered set counts as different). | Client bug; mint a new id. |
| 422 | `insufficient_funds` | A debited leg exceeds that account's balance. Nothing was appended on any stream. | Business rejection. Do not retry unchanged. |
| 400 | `unbalanced` | Debits ≠ credits, or fewer than two legs. | Fix the request. |
| 400 | `duplicate_account` | One account appears in more than one leg. | Fix the request. |
| 400 | `too_many_legs` | More than 16 legs. | Split the intent into several posting sets (each is atomic on its own). |
| 400 | `anchor_not_debited` | The path account is not a debited leg. | Anchor on an account whose money leaves. |
| 400 | `domain_rejected` + a domain code | E.g. `amount_out_of_range` when a credit would overflow. | Fix the request. |

### Domain error codes (400 unless noted)

| `error_code` | Meaning |
|---|---|
| `invalid_account_id` | Empty or malformed account id. |
| `invalid_amount` | Amount is not a positive integer (minor units). |
| `invalid_expected_version` | `expected_version` is negative or not an integer. |
| `amount_out_of_range` | Amount exceeds the signed 64-bit range the ledger stores. |
| `version_out_of_range` | Version exceeds the signed 64-bit range. |
| `invalid_account_state` | Internal invariant violated while folding the stream (report it). |
| `account_identity_mismatch` | Event's account id differs from the stream's (report it). |
| `insufficient_funds` | See 422 above. |
| `same_account` | A transfer named the same account as source and target. |
| `unbalanced` | A posting set's debits and credits differ, or it has fewer than two legs. |
| `duplicate_account` | A posting set names one account in more than one leg. |
| `too_many_legs` | A posting set has more than 16 legs. |
| `anchor_not_debited` | A posting set's anchor (path account) is not a debited leg. |
| `unknown_command` | `type` is not `deposit` or `withdraw`. |
| `unknown_event` | Stream holds an event type the aggregate cannot fold (report it). |
| `domain_error` | Base code; only seen if a new rule forgot its own code. |

## Cross-cutting (any endpoint)

| Status | Body `detail` | Meaning | Client action |
|---|---|---|---|
| 401 | `invalid or expired bearer token` | Always the same text, whatever the cause (no oracle). | Obtain a fresh token. If it persists, check issuer/audience/algorithm configuration. |
| 403 | `principal is not authorized for this account` | Token is valid but not permitted for this account. | Register ownership (`POST /v1/accounts`) or use a token carrying the account / admin scope. |
| 404 | `account not found` | Balance read before any event was projected for the account. | Not an error after a write: poll until the projection catches up (see 201). |
| 413 | `request body exceeds N bytes` | Body exceeds `CLOUDSCALE_MAX_BODY_BYTES` (16 KiB default). | Commands are ~150 bytes; this is a client bug. |
| 429 | `rate limit exceeded` + `Retry-After` | Per-client (pre-auth) or per-subject budget exhausted. | Sleep `Retry-After` seconds, then retry (same `command_id`). |
| 501 | `account registration is not enabled` | Deployment has no ownership registry configured. | Use claim-based or admin tokens. |
| 503 | `command path unavailable (circuit open)` + `Retry-After` | Storage was failing; breaker is open. | Sleep `Retry-After`, retry with the **same** `command_id`. |
| 503 | `command path unavailable (transient storage failure)` | Retries exhausted against a flaky store, or the pool was exhausted. | Same as above. |
| 503 | `command path unavailable (event schema newer than this build)` | Stream holds an event written by a newer build (rolled-back deploy). | Sleep `Retry-After` (60 s). Operators must redeploy the newer build; the account is frozen, not corrupted. |
| 503 | `/v1/ready` → `{"status":"not_ready","checks":{"error":…}}` | Replica cannot reach storage or schema revision mismatches. | Orchestrator concern: route elsewhere. The message is in the server log, not the body. |
| 500 | `Internal Server Error` | A bug. No detail is exposed. | Report with the `X-Request-Id` from the access log. |

## Registration endpoint (`POST /v1/accounts`)

| Status | Meaning |
|---|---|
| 201 | Ownership registered to the caller's subject. |
| 200 | Already registered to this subject (idempotent). |
| 409 | Registered to a different subject. |
| 400 | `invalid_account_id`. |

`examples/python_client.py` implements every client action above.
