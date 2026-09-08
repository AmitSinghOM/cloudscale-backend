"""Concrete ``CommandUnitOfWork``: decide, append, persist — atomically, on SQLite.

This is the write-side heart of the typed path. One ``BEGIN IMMEDIATE``
transaction covers the whole decision: read the stored result (idempotency),
fold the stream, gate on ``expected_version``, run the aggregate's pure
``decide``, append the event, and persist the result. SQLite's write lock
serializes competing writers for the duration, so the fold can never go stale
between read and append.

Port contract, exactly as ``application.ports.CommandUnitOfWork`` states:

- A replay with an **equal request hash** returns the original persisted
  result byte-for-byte (reconstructed via ``CommandResult.from_dict``).
- The **same command_id with a different hash** returns
  ``command_id_conflict`` (409) and is NOT persisted — the original result
  remains the command's one true record.
- **Deterministic rejections** (version conflict, insufficient funds, other
  domain errors) are persisted WITHOUT appending, so their replays are also
  stable.

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

from cloudscale.application.ports import NormalizedCommand
from cloudscale.domain.account import AccountState, decide, fold
from cloudscale.domain.errors import DomainError, InsufficientFundsError
from cloudscale.domain.events import Deposited, EventEnvelope, Withdrawn
from cloudscale.domain.results import CommandOutcome, CommandResult

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
                result = self._execute_locked(request)
                self._conn.commit()
                return result
            except BaseException:
                self._conn.rollback()
                raise

    # -- internals -------------------------------------------------------------

    def _execute_locked(self, request: NormalizedCommand) -> CommandResult:
        stored = self._stored_result(request.command_id)
        if stored is not None:
            if stored.request_hash == request.request_hash:
                return stored
            # Different business request under a reused command id. Reject,
            # but never overwrite the original persisted record.
            return self._rejection(
                request,
                CommandOutcome.COMMAND_ID_CONFLICT,
                error_code="command_id_conflict",
                http_status=409,
                current_version=self._fold_stream(request.command.account_id).version,
            )

        state = self._fold_stream(request.command.account_id)

        if request.command.expected_version != state.version:
            return self._persist(
                self._rejection(
                    request,
                    CommandOutcome.VERSION_CONFLICT,
                    error_code="version_conflict",
                    http_status=409,
                    current_version=state.version,
                )
            )

        try:
            event = decide(state, request.command)
        except InsufficientFundsError as error:
            return self._persist(
                self._rejection(
                    request,
                    CommandOutcome.INSUFFICIENT_FUNDS,
                    error_code=error.code,
                    http_status=422,
                    current_version=state.version,
                )
            )
        except DomainError as error:
            return self._persist(
                self._rejection(
                    request,
                    CommandOutcome.DOMAIN_REJECTED,
                    error_code=error.code,
                    http_status=400,
                    current_version=state.version,
                )
            )

        occurred_at = self._clock()
        envelope = EventEnvelope.from_domain_event(
            event,
            event_id=self._event_id_factory(),
            stream_version=state.version + 1,
            occurred_at=occurred_at,
            correlation_id=request.correlation_id,
            causation_id=request.command_id,
            command_id=request.command_id,
        )
        self._append(envelope, event)
        return self._persist(
            CommandResult(
                command_id=request.command_id,
                request_hash=request.request_hash,
                outcome=CommandOutcome.ACCEPTED,
                account_id=request.command.account_id,
                expected_version=request.command.expected_version,
                current_version=state.version,
                committed_version=envelope.stream_version,
                event_id=envelope.event_id,
                correlation_id=request.correlation_id,
                error_code=None,
                http_status=201,
                created_at=occurred_at,
            )
        )

    def _stored_result(self, command_id: UUID) -> CommandResult | None:
        row = self._conn.execute(
            "SELECT result_json FROM command_results WHERE command_id = ?",
            (str(command_id),),
        ).fetchone()
        if row is None:
            return None
        return CommandResult.from_dict(json.loads(row["result_json"]))

    def _fold_stream(self, account_id: str) -> AccountState:
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

    def _append(self, envelope: EventEnvelope, event: Deposited | Withdrawn) -> None:
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

    def _persist(self, result: CommandResult) -> CommandResult:
        self._conn.execute(
            "INSERT INTO command_results (command_id, request_hash, result_json) "
            "VALUES (?, ?, ?)",
            (str(result.command_id), result.request_hash, result.to_json()),
        )
        return result

    def _rejection(
        self,
        request: NormalizedCommand,
        outcome: CommandOutcome,
        *,
        error_code: str,
        http_status: int,
        current_version: int,
    ) -> CommandResult:
        return CommandResult(
            command_id=request.command_id,
            request_hash=request.request_hash,
            outcome=outcome,
            account_id=request.command.account_id,
            expected_version=request.command.expected_version,
            current_version=current_version,
            committed_version=None,
            event_id=None,
            correlation_id=request.correlation_id,
            error_code=error_code,
            http_status=http_status,
            created_at=self._clock(),
        )


__all__ = ["SqliteCommandUnitOfWork"]
