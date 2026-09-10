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

from cloudscale.application.command_execution import execute_command_decision
from cloudscale.application.ports import NormalizedCommand
from cloudscale.domain.account import AccountState, fold
from cloudscale.domain.events import AccountEvent, Deposited, EventEnvelope, Withdrawn
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
    UNIQUE (stream, seq),
    UNIQUE (event_id)
);
"""

_UOW_SCHEMA = """
CREATE TABLE IF NOT EXISTS command_results (
    command_id   TEXT PRIMARY KEY,
    request_hash BLOB NOT NULL,
    result_json  TEXT NOT NULL
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
"""


def _stream_name(account_id: str) -> str:
    """Match the legacy handler's stream naming so logs stay unified."""
    return f"account-{account_id}"


class SqliteCommandUnitOfWork:
    """Atomic command execution over the shared durable SQLite log."""

    def __init__(
        self,
        path: str,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        event_id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        try:
            self._conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError:
            pass
        self._conn.executescript(_EVENTS_SCHEMA + _UOW_SCHEMA)
        self._conn.commit()
        self._clock = clock
        self._event_id_factory = event_id_factory
        self._lock = threading.Lock()

    def close(self) -> None:
        self._conn.close()

    def execute(self, request: NormalizedCommand) -> CommandResult:
        with self._lock:
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
        rows = self._conn.execute(
            "SELECT type, account_id, amount FROM events "
            "WHERE stream = ? ORDER BY seq ASC",
            (_stream_name(account_id),),
        ).fetchall()
        events: list[Deposited | Withdrawn] = []
        for row in rows:
            if row["type"] == "Deposited":
                events.append(
                    Deposited(account_id=row["account_id"], amount=row["amount"])
                )
            elif row["type"] == "Withdrawn":
                events.append(
                    Withdrawn(account_id=row["account_id"], amount=row["amount"])
                )
            else:  # pragma: no cover - typed path never writes other types
                raise ValueError(f"unsupported event type in stream: {row['type']!r}")
        return fold(events)

    def append_event(self, envelope: EventEnvelope, event: AccountEvent) -> None:
        self._conn.execute(
            "INSERT INTO events (event_id, stream, seq, type, account_id, amount) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                str(envelope.event_id),
                _stream_name(event.account_id),
                envelope.stream_version,
                envelope.event_type,
                event.account_id,
                event.amount,
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

    def persist_result(self, result: CommandResult) -> None:
        self._conn.execute(
            "INSERT INTO command_results (command_id, request_hash, result_json) "
            "VALUES (?, ?, ?)",
            (str(result.command_id), result.request_hash, result.to_json()),
        )


__all__ = ["SqliteCommandUnitOfWork"]
