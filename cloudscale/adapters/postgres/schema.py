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
CURRENT_REVISION = "0002_command_results_created_at"

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

PROJECTION = """
CREATE TABLE IF NOT EXISTS balances (
    account_id TEXT PRIMARY KEY,
    balance    BIGINT NOT NULL DEFAULT 0,
    version    BIGINT NOT NULL DEFAULT 0
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
ALL = (EVENTS, OUTBOX, COMMAND_RESULTS, PROJECTION, ACCOUNTS, RATE_LIMIT)

__all__ = [
    "ACCOUNTS",
    "ALL",
    "COMMAND_RESULTS",
    "CURRENT_REVISION",
    "EVENTS",
    "OUTBOX",
    "PROJECTION",
    "RATE_LIMIT",
]
