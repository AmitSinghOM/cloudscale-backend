# Architecture Decision Records

One record per load-bearing decision. Records are **immutable once
accepted**; to change a decision, write a new ADR with status *Supersedes
ADR-NNNN* and set the old one to *Superseded by ADR-MMMM*.

Why this exists: a maintainer in 2033 must be able to tell a *constraint*
("this must hold or money is lost") from an *accident* ("this was easiest at
the time"). Commit messages carry the what; ADRs carry the why and the
alternatives rejected.

A pull request that changes a public contract — HTTP route or response,
event shape, table, environment variable, exit code — must reference an ADR
or add one. Reviewers reject otherwise.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-hexagonal-core-enforced-by-tests.md) | Hexagonal core enforced by tests | Accepted |
| [0002](0002-event-log-as-sole-source-of-record.md) | Event log as sole source of record | Accepted |
| [0003](0003-idempotent-command-unit-of-work.md) | Idempotent command unit of work | Accepted |
| [0004](0004-two-storage-tiers-behind-shared-ports.md) | Two storage tiers behind shared ports | Accepted |
| [0005](0005-transactional-outbox-on-postgresql.md) | Transactional outbox on PostgreSQL | Accepted |
| [0006](0006-consumer-ha-via-session-advisory-lock.md) | Consumer HA via session advisory lock | Accepted |
| [0007](0007-fail-closed-migrations-in-production.md) | Fail-closed migrations in production | Accepted |
| [0008](0008-exact-pins-hash-verified-installs.md) | Exact pins and hash-verified installs | Accepted |
| [0009](0009-event-schema-evolution-via-upcasters.md) | Event schema evolution via upcasters | Accepted |
| [0010](0010-python-version-policy.md) | Python version policy | Accepted |
| [0011](0011-double-entry-transfer.md) | Double-entry Transfer as two postings in one transaction | Accepted |
| [0012](0012-stream-snapshots-verified-cache.md) | Stream snapshots as a verified cache over the event log | Accepted |
| [0013](0013-n-leg-balanced-postings.md) | N-leg balanced postings on the transfer_id grouping | Accepted |
| [0014](0014-pending-transfers-holds.md) | Pending transfers (holds) as reserve, post, void, expire | Accepted |
| [0015](0015-revert-as-mirror-posting-set.md) | Revert as a mirror posting set linked to the original | Accepted |
| [0016](0016-restore-drill-as-executable-gate.md) | Restore drill as an executable gate over a classified schema | Proposed |

Template: [`TEMPLATE.md`](TEMPLATE.md).
