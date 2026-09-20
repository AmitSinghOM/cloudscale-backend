"""The PostgreSQL schema, defined once.

Both consumers of this module must agree or the tier is unsafe:

- adapters in ``auto`` schema mode run these statements at startup
  (``CREATE ... IF NOT EXISTS``, dev/test convenience);
- Alembic migration ``0001_initial`` runs the same statements — production
  applies migrations explicitly and adapters in ``migrations`` mode only
  VERIFY the revision, never create.

A PG-gated test builds a database each way and asserts the resulting
information_schema is identical.
"""

from __future__ import annotations

#: Alembic revision the adapters require in ``migrations`` schema mode.
CURRENT_REVISION = "0007_reverts"

EVENTS = """
CREATE TABLE IF NOT EXISTS events (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id   TEXT NOT NULL UNIQUE,
    stream     TEXT NOT NULL,
    seq        BIGINT NOT NULL,
    type       TEXT,
    account_id TEXT,
    amount     BIGINT,
    UNIQUE (stream, seq)
);
ALTER TABLE events ADD COLUMN IF NOT EXISTS published BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE events ADD COLUMN IF NOT EXISTS schema_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE events ADD COLUMN IF NOT EXISTS transfer_id TEXT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS counterparty TEXT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS expires_at TEXT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS release_reason TEXT;
CREATE INDEX IF NOT EXISTS events_stream_transfer_idx ON events (stream, transfer_id)
    WHERE transfer_id IS NOT NULL;
ALTER TABLE events ADD COLUMN IF NOT EXISTS reverts TEXT;
CREATE INDEX IF NOT EXISTS events_transfer_idx ON events (transfer_id)
    WHERE transfer_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS events_reverts_idx ON events (reverts)
    WHERE reverts IS NOT NULL;
CREATE INDEX IF NOT EXISTS events_unpublished_idx ON events (id) WHERE NOT published;
"""

OUTBOX = """
CREATE TABLE IF NOT EXISTS outbox (
    position BIGINT PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE REFERENCES events(event_id)
);
"""

COMMAND_RESULTS = """
CREATE TABLE IF NOT EXISTS command_results (
    command_id   TEXT PRIMARY KEY,
    request_hash BYTEA NOT NULL,
    result_json  TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS command_results_created_at_idx
    ON command_results (created_at);
CREATE TABLE IF NOT EXISTS event_envelopes (
    event_id       TEXT PRIMARY KEY,
    stream_id      TEXT NOT NULL,
    stream_version BIGINT NOT NULL,
    event_type     TEXT NOT NULL,
    occurred_at    TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    causation_id   TEXT NOT NULL,
    command_id     TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    UNIQUE (stream_id, stream_version)
);
"""

#: ADR-0012: at most one verified-cache row per stream; never the source of record.
SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS stream_snapshots (
    stream          TEXT PRIMARY KEY,
    seq             BIGINT NOT NULL,
    state_json      TEXT NOT NULL,
    state_version   INTEGER NOT NULL,
    anchor_event_id TEXT NOT NULL
);
"""

PROJECTION = """
CREATE TABLE IF NOT EXISTS balances (
    account_id TEXT PRIMARY KEY,
    balance    BIGINT NOT NULL DEFAULT 0,
    version    BIGINT NOT NULL DEFAULT 0
);
ALTER TABLE balances ADD COLUMN IF NOT EXISTS held BIGINT NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS holds (
    hold_id    TEXT PRIMARY KEY,
    source     TEXT NOT NULL,
    target     TEXT NOT NULL,
    amount     BIGINT NOT NULL,
    expires_at TEXT NOT NULL,
    state      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS holds_open_expiry_idx ON holds (expires_at) WHERE state = 'open';
CREATE TABLE IF NOT EXISTS transfers (
    transfer_id TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,
    reverts     TEXT,
    reverted_by TEXT
);
CREATE TABLE IF NOT EXISTS transfer_legs (
    transfer_id TEXT NOT NULL,
    account_id  TEXT NOT NULL,
    amount      BIGINT NOT NULL,
    direction   TEXT NOT NULL,
    PRIMARY KEY (transfer_id, account_id)
);
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS consumer_offset (
    consumer TEXT PRIMARY KEY,
    last_id  BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS dead_letters (
    event_id         TEXT PRIMARY KEY,
    log_id           BIGINT NOT NULL,
    payload          TEXT NOT NULL,
    error_type       TEXT NOT NULL,
    error_message    TEXT NOT NULL,
    attempts         INTEGER NOT NULL,
    dead_lettered_at TEXT NOT NULL
);
"""

ACCOUNTS = """
CREATE TABLE IF NOT EXISTS accounts (
    account_id    TEXT PRIMARY KEY,
    owner_subject TEXT NOT NULL,
    created_at    TEXT NOT NULL
);
"""

RATE_LIMIT = """
CREATE TABLE IF NOT EXISTS rate_limit_buckets (
    bucket_key TEXT PRIMARY KEY,
    tokens     DOUBLE PRECISION NOT NULL,
    updated_at DOUBLE PRECISION NOT NULL
);
"""

#: Every statement group, in dependency order (outbox references events).
ALL = (EVENTS, OUTBOX, COMMAND_RESULTS, SNAPSHOTS, PROJECTION, ACCOUNTS, RATE_LIMIT)

# -- Table classification (ADR-0016) -----------------------------------------
#
# Every table belongs to exactly one class. The classes are what a backup must
# contain and what a restore may throw away; RUNBOOK R7 is checked against
# them, the migrated schema is checked against them, and the restore drill
# truncates exactly ``DERIVED`` before rebuilding. A new table that is not
# classified fails the build.

#: Lost data is lost money, idempotency, identity or ownership. The backup
#: is these. ``accounts`` is here because registrations are written outside
#: the log (see ADR-0016, alternatives) and cannot be rebuilt from it.
SYSTEM_OF_RECORD: frozenset[str] = frozenset(
    {"events", "event_envelopes", "command_results", "accounts"}
)

#: Rebuilt from ``events`` by the consumer (read models, dedupe, offset,
#: dead letters), by the relay (``outbox`` and the ``events.published``
#: column) or by the next fold (``stream_snapshots``, ADR-0012).
DERIVED: frozenset[str] = frozenset(
    {
        "outbox",
        "stream_snapshots",
        "balances",
        "holds",
        "transfers",
        "transfer_legs",
        "processed_events",
        "consumer_offset",
        "dead_letters",
    }
)

#: Neither backed up nor rebuilt; a restart forgives a rate-limit budget.
EPHEMERAL: frozenset[str] = frozenset({"rate_limit_buckets"})

#: Alembic's own bookkeeping; verified through ``migrate current``, not ours.
TOOLING: frozenset[str] = frozenset({"alembic_version"})

#: Derived tables the consumer rebuilds from the log, in the order the drill
#: truncates them (no foreign keys among them; order is for readable reports).
CONSUMER_REBUILT: tuple[str, ...] = (
    "balances",
    "holds",
    "transfers",
    "transfer_legs",
    "processed_events",
    "consumer_offset",
    "dead_letters",
)


def classify(table: str) -> str:
    """Return the class of ``table`` or raise: an unclassified table is a build error."""
    for name, members in (
        ("system_of_record", SYSTEM_OF_RECORD),
        ("derived", DERIVED),
        ("ephemeral", EPHEMERAL),
        ("tooling", TOOLING),
    ):
        if table in members:
            return name
    raise KeyError(f"table {table!r} is not classified (ADR-0016)")


__all__ = [
    "ACCOUNTS",
    "ALL",
    "COMMAND_RESULTS",
    "CONSUMER_REBUILT",
    "CURRENT_REVISION",
    "DERIVED",
    "EPHEMERAL",
    "EVENTS",
    "OUTBOX",
    "PROJECTION",
    "RATE_LIMIT",
    "SNAPSHOTS",
    "SYSTEM_OF_RECORD",
    "TOOLING",
    "classify",
]
