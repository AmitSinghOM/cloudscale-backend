"""Concrete ``CommandUnitOfWork`` on PostgreSQL — pooled, same contract as SQLite.

The decision logic lives in ``application.command_execution``. This adapter
checks one connection out of the pool per ``execute`` and hands the decision
a ``_BoundStorage`` view whose four operations all run on that connection,
inside that connection's transaction — so the fold, the append, and the
result persistence are atomic together.

No process-wide lock: concurrent executes run on separate connections and
the database's UNIQUE constraints arbitrate. A lost race (stream version or
command id) raises UniqueViolation, the transaction rolls back, and a
bounded retry re-runs the decision on fresh state.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import DictRow

from cloudscale.adapters.postgres import schema
from cloudscale.adapters.postgres.pool import ensure_schema, open_pool
from cloudscale.application.command_execution import execute_command_decision
from cloudscale.application.ports import NormalizedCommand
from cloudscale.domain.account import AccountState, fold
from cloudscale.domain.events import AccountEvent, Deposited, EventEnvelope, Withdrawn
from cloudscale.domain.results import CommandResult

# Same events DDL as PostgresEventStore so both writers interoperate.
_SCHEMA = schema.EVENTS + schema.COMMAND_RESULTS

_MAX_RACE_RETRIES = 2


def _stream_name(account_id: str) -> str:
    return f"account-{account_id}"


class _BoundStorage:
    """``CommandDecisionStorage`` over one checked-out connection."""

    def __init__(self, conn: psycopg.Connection[DictRow]) -> None:
        self._conn = conn

    def stored_result(self, command_id: UUID) -> CommandResult | None:
        row = self._conn.execute(
            "SELECT result_json FROM command_results WHERE command_id = %s",
            (str(command_id),),
        ).fetchone()
        if row is None:
            return None
        return CommandResult.from_dict(json.loads(row["result_json"]))

    def fold_stream(self, account_id: str) -> AccountState:
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

    def append_event(self, envelope: EventEnvelope, event: AccountEvent) -> None:
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

    def persist_result(self, result: CommandResult) -> None:
        self._conn.execute(
            "INSERT INTO command_results (command_id, request_hash, result_json) "
            "VALUES (%s, %s, %s)",
            (str(result.command_id), result.request_hash, result.to_json()),
        )


class PostgresCommandUnitOfWork:
    """Atomic command execution over the PostgreSQL durable log."""

    def __init__(
        self,
        conninfo: str,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        event_id_factory: Callable[[], UUID] = uuid4,
        pool_max: int | None = None,
    ) -> None:
        self._pool = open_pool(conninfo, max_size=pool_max)
        ensure_schema(self._pool, _SCHEMA)
        self._clock = clock
        self._event_id_factory = event_id_factory

    def close(self) -> None:
        self._pool.close()

    def execute(self, request: NormalizedCommand) -> CommandResult:
        with self._pool.connection() as conn:
            storage = _BoundStorage(conn)
            for attempt in range(1, _MAX_RACE_RETRIES + 2):
                try:
                    with conn.transaction():
                        return execute_command_decision(
                            storage,
                            request,
                            clock=self._clock,
                            event_id_factory=self._event_id_factory,
                        )
                except psycopg.errors.UniqueViolation:
                    if attempt > _MAX_RACE_RETRIES:
                        raise
        raise AssertionError("unreachable")  # pragma: no cover


__all__ = ["PostgresCommandUnitOfWork"]
