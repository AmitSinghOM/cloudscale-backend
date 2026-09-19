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

from cloudscale.adapters.compat import event_row_fields, legacy_event_to_domain
from cloudscale.adapters.postgres import schema
from cloudscale.adapters.postgres.pool import ensure_schema, open_pool
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
from cloudscale.domain.account import (
    AccountState,
    OpenHold,
    fold,
    open_hold_from_events,
)
from cloudscale.domain.events import (
    AccountEvent,
    EventEnvelope,
    HoldPlaced,
    HoldPosted,
    HoldReleased,
)
from cloudscale.domain.upcasting import upcast
from cloudscale.domain.results import CommandResult

# Same events DDL as PostgresEventStore so both writers interoperate.
_SCHEMA = schema.EVENTS + schema.COMMAND_RESULTS + schema.SNAPSHOTS

_MAX_RACE_RETRIES = 2

_SELECT_FROM_SEQ = (
    "SELECT seq, event_id, type, account_id, amount, transfer_id, counterparty, "
    "expires_at, release_reason, schema_version FROM events "
    "WHERE stream = %s AND seq >= %s ORDER BY seq ASC"
)
_SELECT_ALL = (
    "SELECT seq, event_id, type, account_id, amount, transfer_id, counterparty, "
    "expires_at, release_reason, schema_version FROM events "
    "WHERE stream = %s ORDER BY seq ASC"
)
_SELECT_HOLD = (
    "SELECT type, account_id, amount, transfer_id, counterparty, expires_at, "
    "release_reason, schema_version FROM events "
    "WHERE stream = %s AND transfer_id = %s AND type IN "
    "('HoldPlaced', 'HoldReleased', 'HoldPosted') ORDER BY seq ASC"
)


def _stream_name(account_id: str) -> str:
    return f"account-{account_id}"


class _BoundStorage:
    """``CommandDecisionStorage`` over one checked-out connection."""

    def __init__(self, conn: psycopg.Connection[DictRow], snapshot_every: int) -> None:
        self._conn = conn
        self._tracker = SnapshotTracker(snapshot_every)

    def stored_result(self, command_id: UUID) -> CommandResult | None:
        row = self._conn.execute(
            "SELECT result_json FROM command_results WHERE command_id = %s",
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
            reason = snapshot_rejection(snapshot, rows)
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
        events = [legacy_event_to_domain(upcast(dict(r))) for r in rows]
        return open_hold_from_events(
            hold_id,
            [
                e
                for e in events
                if isinstance(e, (HoldPlaced, HoldReleased, HoldPosted))
            ],
        )

    def _read_snapshot(self, stream: str) -> StreamSnapshot | None:
        row = self._conn.execute(
            "SELECT seq, state_json, state_version, anchor_event_id "
            "FROM stream_snapshots WHERE stream = %s",
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
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (stream) DO UPDATE SET seq = EXCLUDED.seq, "
            "state_json = EXCLUDED.state_json, state_version = EXCLUDED.state_version, "
            "anchor_event_id = EXCLUDED.anchor_event_id "
            "WHERE stream_snapshots.seq < EXCLUDED.seq",
            (
                stream,
                snapshot.seq,
                snapshot.to_state_json(),
                snapshot.state_version,
                snapshot.anchor_event_id,
            ),
        )

    def append_event(self, envelope: EventEnvelope, event: AccountEvent) -> None:
        transfer_id, counterparty, expires_at, reason = event_row_fields(event)
        self._conn.execute(
            "INSERT INTO events "
            "(event_id, stream, seq, type, account_id, amount, schema_version, "
            "transfer_id, counterparty, expires_at, release_reason) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
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
        due = self._tracker.after_append(envelope, event)
        if due is not None:
            self._write_snapshot(_stream_name(event.account_id), due)

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
        snapshot_every: int = DEFAULT_SNAPSHOT_EVERY,
    ) -> None:
        self._pool = open_pool(conninfo, max_size=pool_max)
        ensure_schema(self._pool, _SCHEMA)
        self._clock = clock
        self._event_id_factory = event_id_factory
        self._snapshot_every = snapshot_every

    def close(self) -> None:
        self._pool.close()

    # -- read-only views (operator tooling, tests) --------------------------------

    def fold_stream(self, account_id: str) -> AccountState:
        """The current state of one stream, folded exactly as a decision would."""
        with self._pool.connection() as conn:
            return _BoundStorage(conn, snapshot_every=0).fold_stream(account_id)

    def open_hold(self, account_id: str, hold_id: UUID) -> OpenHold | None:
        with self._pool.connection() as conn:
            return _BoundStorage(conn, snapshot_every=0).open_hold(account_id, hold_id)

    def execute(self, request: NormalizedCommand) -> CommandResult:
        with self._pool.connection() as conn:
            for attempt in range(1, _MAX_RACE_RETRIES + 2):
                # A fresh tracker per attempt: a retried decision refolds.
                storage = _BoundStorage(conn, self._snapshot_every)
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
