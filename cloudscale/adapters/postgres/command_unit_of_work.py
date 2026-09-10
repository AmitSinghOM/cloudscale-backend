"""Concrete ``CommandUnitOfWork`` on PostgreSQL — same contract as SQLite.

Single-connection instances serialize in-process with a lock; ACROSS
processes the database's UNIQUE constraints arbitrate instead of any lock:

- Two writers racing the same stream version both pass the pre-append fold,
  and UNIQUE(stream, seq) rejects the loser. The loser's transaction rolls
  back and the execute retries once — the fresh fold then yields the honest
  ``version_conflict`` result (persisted, no append).
- Two writers racing the same ``command_id`` collide on the
  ``command_results`` PRIMARY KEY; the loser's retry finds the winner's
  stored result and applies the normal replay/conflict semantics.

Everything else — equal-hash replay identity, conflict-not-persisted,
rejections-persisted-without-append, envelope retention — mirrors
``SqliteCommandUnitOfWork``.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row

from cloudscale.application.ports import NormalizedCommand
from cloudscale.domain.account import AccountState, decide, fold
from cloudscale.domain.errors import DomainError, InsufficientFundsError
from cloudscale.domain.events import Deposited, EventEnvelope, Withdrawn
from cloudscale.domain.results import CommandOutcome, CommandResult

# Same events DDL as PostgresEventStore so both writers interoperate.
_SCHEMA = """
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
CREATE TABLE IF NOT EXISTS command_results (
    command_id   TEXT PRIMARY KEY,
    request_hash BYTEA NOT NULL,
    result_json  TEXT NOT NULL
);
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

_MAX_RACE_RETRIES = 2


def _stream_name(account_id: str) -> str:
    return f"account-{account_id}"


class PostgresCommandUnitOfWork:
    """Atomic command execution over the PostgreSQL durable log."""

    def __init__(
        self,
        conninfo: str,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        event_id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        # Autocommit connection: psycopg3's recommended pattern. Without it, a
        # bare read opens an implicit transaction and a later
        # conn.transaction() block silently degrades to a SAVEPOINT that
        # never commits (writes lost on close). With autocommit=True every
        # transaction() block is a REAL transaction and single statements
        # commit immediately.
        self._conn = psycopg.connect(conninfo, row_factory=dict_row, autocommit=True)
        with self._conn.transaction():
            # Serialize concurrent schema creation across processes:
            # simultaneous CREATE TABLE IF NOT EXISTS can fail on the
            # pg_type unique index when server + consumer start together.
            self._conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext('cloudscale_schema'))"
            )
            self._conn.execute(_SCHEMA)
        self._clock = clock
        self._event_id_factory = event_id_factory
        self._lock = threading.Lock()

    def close(self) -> None:
        self._conn.close()

    def execute(self, request: NormalizedCommand) -> CommandResult:
        with self._lock:
            for attempt in range(1, _MAX_RACE_RETRIES + 2):
                try:
                    with self._conn.transaction():
                        return self._execute_in_transaction(request)
                except psycopg.errors.UniqueViolation:
                    # Cross-process race lost: either another writer took our
                    # stream seq or persisted our command_id first. The next
                    # pass folds fresh state / finds the stored result.
                    if attempt > _MAX_RACE_RETRIES:
                        raise
        raise AssertionError("unreachable")  # pragma: no cover

    # -- internals -------------------------------------------------------------

    def _execute_in_transaction(self, request: NormalizedCommand) -> CommandResult:
        stored = self._stored_result(request.command_id)
        if stored is not None:
            if stored.request_hash == request.request_hash:
                return stored
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
            "SELECT result_json FROM command_results WHERE command_id = %s",
            (str(command_id),),
        ).fetchone()
        if row is None:
            return None
        return CommandResult.from_dict(json.loads(row["result_json"]))

    def _fold_stream(self, account_id: str) -> AccountState:
        rows = self._conn.execute(
            "SELECT type, account_id, amount FROM events "
            "WHERE stream = %s ORDER BY seq ASC",
            (_stream_name(account_id),),
        ).fetchall()
        events: list[Deposited | Withdrawn] = []
        for row in rows:
            if row["type"] == "Deposited":
                events.append(
                    Deposited(account_id=row["account_id"], amount=int(row["amount"]))
                )
            elif row["type"] == "Withdrawn":
                events.append(
                    Withdrawn(account_id=row["account_id"], amount=int(row["amount"]))
                )
            else:  # pragma: no cover - typed path never writes other types
                raise ValueError(f"unsupported event type in stream: {row['type']!r}")
        return fold(events)

    def _append(self, envelope: EventEnvelope, event: Deposited | Withdrawn) -> None:
        self._conn.execute(
            "INSERT INTO events (event_id, stream, seq, type, account_id, amount) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
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
            "schema_version) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
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
            "VALUES (%s, %s, %s)",
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


__all__ = ["PostgresCommandUnitOfWork"]
