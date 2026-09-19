"""Concrete ``CommandUnitOfWork``: decide, append, persist — atomically, on SQLite.

The decision logic lives in ``application.command_execution`` (shared with
the Postgres adapter); this class supplies the four storage operations and
the atomicity: one ``BEGIN IMMEDIATE`` transaction covers the whole decision,
and SQLite's write lock serializes competing writers for its duration, so
the fold can never go stale between read and append.

Events land in the same ``events`` table the legacy ``SqliteEventStore``
serves (identical DDL), so the projection consumer sees typed-path writes
with no extra plumbing. Full envelope identity (correlation, causation,
occurred_at, schema_version) is retained in ``event_envelopes``.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID, uuid4

from cloudscale.adapters.compat import (
    event_row_fields,
    legacy_event_to_domain,
    open_hold_from_rows,
)
from cloudscale.application.command_execution import execute_command_decision
from cloudscale.application.ports import NormalizedCommand
from cloudscale.application.snapshots import (
    DEFAULT_SNAPSHOT_EVERY,
    SnapshotTracker,
    StreamSnapshot,
    fold_from_snapshot,
    log_snapshot_rejected,
    snapshot_rejection,
)
from cloudscale.domain.account import AccountState, OpenHold, fold
from cloudscale.domain.events import AccountEvent, EventEnvelope
from cloudscale.domain.upcasting import upcast
from cloudscale.domain.results import CommandResult

# Identical to cqrs.durable_eventstore._SCHEMA so both writers interoperate
# on the same file; CREATE IF NOT EXISTS makes creation order irrelevant.
_EVENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id   TEXT NOT NULL,
    stream     TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    type       TEXT NOT NULL,
    account_id TEXT,
    amount     INTEGER,
    schema_version INTEGER NOT NULL DEFAULT 1,
    UNIQUE (stream, seq),
    UNIQUE (event_id)
);
"""

_UOW_SCHEMA = """
CREATE TABLE IF NOT EXISTS command_results (
    command_id   TEXT PRIMARY KEY,
    request_hash BLOB NOT NULL,
    result_json  TEXT NOT NULL,
    created_at   REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS event_envelopes (
    event_id       TEXT PRIMARY KEY,
    stream_id      TEXT NOT NULL,
    stream_version INTEGER NOT NULL,
    event_type     TEXT NOT NULL,
    occurred_at    TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    causation_id   TEXT NOT NULL,
    command_id     TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    UNIQUE (stream_id, stream_version)
);

CREATE TABLE IF NOT EXISTS stream_snapshots (
    stream          TEXT PRIMARY KEY,
    seq             INTEGER NOT NULL,
    state_json      TEXT NOT NULL,
    state_version   INTEGER NOT NULL,
    anchor_event_id TEXT NOT NULL
);
"""


def _add_created_at_if_missing(conn: sqlite3.Connection) -> None:
    """In-place upgrade for databases created before retention existed.

    SQLite has no ``ADD COLUMN IF NOT EXISTS``; inspect first. Existing rows
    are stamped "now" (treated as fresh) — the same choice the PostgreSQL
    migration makes. ``CREATE TABLE IF NOT EXISTS`` above is a no-op for an
    existing table, so the column must be added here.
    """
    columns = {row[1] for row in conn.execute("PRAGMA table_info(command_results)")}
    if "created_at" not in columns:
        conn.execute(
            "ALTER TABLE command_results ADD COLUMN created_at REAL NOT NULL DEFAULT 0"
        )
        # Stamp pre-existing rows "now": treat them as fresh, like the PG migration.
        conn.execute("UPDATE command_results SET created_at = unixepoch('subsec')")
    # Index lives here, not in the CREATE script: on a legacy file the column
    # does not exist until the ALTER above has run.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS command_results_created_at_idx "
        "ON command_results (created_at)"
    )
    # Same treatment for events.schema_version (ADR-0009): legacy files read
    # as v1, which is the first shape by definition.
    event_columns = {row[1] for row in conn.execute("PRAGMA table_info(events)")}
    if "schema_version" not in event_columns:
        conn.execute(
            "ALTER TABLE events ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1"
        )
    # Transfer legs (ADR-0011) pair with each other on the row; NULL elsewhere.
    # Holds (ADR-0014) add their expiry and release reason the same way.
    # A reversal names the set it mirrors (ADR-0015).
    for column in (
        "transfer_id",
        "counterparty",
        "expires_at",
        "release_reason",
        "reverts",
    ):
        if column not in event_columns:
            conn.execute(f"ALTER TABLE events ADD COLUMN {column} TEXT")
    # Serve legs_of / reverted_by (ADR-0015) and the open-hold derivation.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS events_transfer_idx ON events (transfer_id) "
        "WHERE transfer_id IS NOT NULL"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS events_reverts_idx ON events (reverts) "
        "WHERE reverts IS NOT NULL"
    )


def _stream_name(account_id: str) -> str:
    """Match the legacy handler's stream naming so logs stay unified."""
    return f"account-{account_id}"


_SELECT_FROM_SEQ = (
    "SELECT seq, event_id, type, account_id, amount, transfer_id, counterparty, "
    "expires_at, release_reason, reverts, schema_version FROM events "
    "WHERE stream = ? AND seq >= ? ORDER BY seq ASC"
)
_SELECT_ALL = (
    "SELECT seq, event_id, type, account_id, amount, transfer_id, counterparty, "
    "expires_at, release_reason, reverts, schema_version FROM events "
    "WHERE stream = ? ORDER BY seq ASC"
)
_SELECT_HOLD = (
    "SELECT type, account_id, amount, transfer_id, counterparty, expires_at, "
    "release_reason, schema_version FROM events "
    "WHERE stream = ? AND transfer_id = ? AND type IN "
    "('HoldPlaced', 'HoldReleased', 'HoldPosted') ORDER BY seq ASC"
)
_SELECT_LEGS = (
    "SELECT type, account_id, amount, transfer_id, counterparty, expires_at, "
    "release_reason, reverts, schema_version FROM events "
    "WHERE transfer_id = ? ORDER BY id ASC"
)
_SELECT_REVERTED_BY = (
    "SELECT transfer_id FROM events WHERE reverts = ? ORDER BY id ASC LIMIT 1"
)


class SqliteCommandUnitOfWork:
    """Atomic command execution over the shared durable SQLite log."""

    def __init__(
        self,
        path: str,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        event_id_factory: Callable[[], UUID] = uuid4,
        snapshot_every: int = DEFAULT_SNAPSHOT_EVERY,
    ) -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        try:
            self._conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError:
            pass
        self._conn.executescript(_EVENTS_SCHEMA + _UOW_SCHEMA)
        _add_created_at_if_missing(self._conn)
        self._conn.commit()
        self._clock = clock
        self._event_id_factory = event_id_factory
        self._snapshot_every = snapshot_every
        self._tracker = SnapshotTracker(snapshot_every)
        self._lock = threading.Lock()

    def close(self) -> None:
        self._conn.close()

    def execute(self, request: NormalizedCommand) -> CommandResult:
        with self._lock:
            self._tracker = SnapshotTracker(self._snapshot_every)
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                result = execute_command_decision(
                    self,
                    request,
                    clock=self._clock,
                    event_id_factory=self._event_id_factory,
                )
                self._conn.commit()
                return result
            except BaseException:
                self._conn.rollback()
                raise

    # -- CommandDecisionStorage ------------------------------------------------

    def stored_result(self, command_id: UUID) -> CommandResult | None:
        row = self._conn.execute(
            "SELECT result_json FROM command_results WHERE command_id = ?",
            (str(command_id),),
        ).fetchone()
        if row is None:
            return None
        return CommandResult.from_dict(json.loads(row["result_json"]))

    def fold_stream(self, account_id: str) -> AccountState:
        """Fold from the verified snapshot plus tail, else the full stream (ADR-0012)."""
        stream = _stream_name(account_id)
        snapshot = self._read_snapshot(stream)
        if snapshot is not None:
            rows = self._conn.execute(
                _SELECT_FROM_SEQ, (stream, snapshot.seq)
            ).fetchall()
            reason = snapshot_rejection(snapshot, [dict(r) for r in rows])
            if reason is None:
                tail = [legacy_event_to_domain(upcast(dict(r))) for r in rows[1:]]
                state = fold_from_snapshot(snapshot, tail)
                self._tracker.folded(account_id, state, snapshot.seq)
                return state
            log_snapshot_rejected(stream, reason)
        rows = self._conn.execute(_SELECT_ALL, (stream,)).fetchall()
        # Translate each stored shape to the current one before folding
        # (ADR-0009); rows predating the column read as v1.
        state = fold(legacy_event_to_domain(upcast(dict(row))) for row in rows)
        self._tracker.folded(account_id, state, 0)
        return state

    def open_hold(self, account_id: str, hold_id: UUID) -> OpenHold | None:
        """Derive the open hold from this stream's own hold events (ADR-0014)."""
        rows = self._conn.execute(
            _SELECT_HOLD, (_stream_name(account_id), str(hold_id))
        ).fetchall()
        return open_hold_from_rows(hold_id, (dict(r) for r in rows))

    def legs_of(self, transfer_id: UUID) -> tuple[AccountEvent, ...]:
        """Every row of the posting set ``transfer_id``, across streams (ADR-0015)."""
        rows = self._conn.execute(_SELECT_LEGS, (str(transfer_id),)).fetchall()
        return tuple(legacy_event_to_domain(upcast(dict(r))) for r in rows)

    def reverted_by(self, transfer_id: UUID) -> UUID | None:
        """The reversal that names ``transfer_id``, if any (ADR-0015)."""
        row = self._conn.execute(_SELECT_REVERTED_BY, (str(transfer_id),)).fetchone()
        return None if row is None else UUID(str(row["transfer_id"]))

    def _read_snapshot(self, stream: str) -> StreamSnapshot | None:
        row = self._conn.execute(
            "SELECT seq, state_json, state_version, anchor_event_id "
            "FROM stream_snapshots WHERE stream = ?",
            (stream,),
        ).fetchone()
        if row is None:
            return None
        return StreamSnapshot(
            seq=int(row["seq"]),
            state=StreamSnapshot.state_from_json(row["state_json"]),
            state_version=int(row["state_version"]),
            anchor_event_id=str(row["anchor_event_id"]),
        )

    def _write_snapshot(self, stream: str, snapshot: StreamSnapshot) -> None:
        # Monotonic: a lagging writer can never move a snapshot backwards.
        self._conn.execute(
            "INSERT INTO stream_snapshots "
            "(stream, seq, state_json, state_version, anchor_event_id) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT (stream) DO UPDATE SET seq = excluded.seq, "
            "state_json = excluded.state_json, state_version = excluded.state_version, "
            "anchor_event_id = excluded.anchor_event_id "
            "WHERE excluded.seq > stream_snapshots.seq",
            (
                stream,
                snapshot.seq,
                snapshot.to_state_json(),
                snapshot.state_version,
                snapshot.anchor_event_id,
            ),
        )

    def append_event(self, envelope: EventEnvelope, event: AccountEvent) -> None:
        transfer_id, counterparty, expires_at, reason, reverts = event_row_fields(event)
        self._conn.execute(
            "INSERT INTO events "
            "(event_id, stream, seq, type, account_id, amount, schema_version, "
            "transfer_id, counterparty, expires_at, release_reason, reverts) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                str(envelope.event_id),
                _stream_name(event.account_id),
                envelope.stream_version,
                envelope.event_type,
                event.account_id,
                event.amount,
                envelope.schema_version,
                transfer_id,
                counterparty,
                expires_at,
                reason,
                reverts,
            ),
        )
        self._conn.execute(
            "INSERT INTO event_envelopes (event_id, stream_id, stream_version, "
            "event_type, occurred_at, correlation_id, causation_id, command_id, "
            "schema_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                str(envelope.event_id),
                envelope.stream_id,
                envelope.stream_version,
                envelope.event_type,
                envelope.occurred_at.isoformat(),
                str(envelope.correlation_id),
                str(envelope.causation_id),
                str(envelope.command_id),
                envelope.schema_version,
            ),
        )
        due = self._tracker.after_append(envelope, event)
        if due is not None:
            self._write_snapshot(_stream_name(event.account_id), due)

    def persist_result(self, result: CommandResult) -> None:
        self._conn.execute(
            "INSERT INTO command_results "
            "(command_id, request_hash, result_json, created_at) VALUES (?, ?, ?, ?)",
            (
                str(result.command_id),
                result.request_hash,
                result.to_json(),
                self._clock().timestamp(),
            ),
        )


__all__ = ["SqliteCommandUnitOfWork"]
