"""PostgreSQL event store: the production realization of the durable log.

Same contract as ``SqliteEventStore`` (append with per-stream 1-based ``seq``,
optimistic concurrency via UNIQUE(stream, seq), stable ``event_id``, ordered
per-stream and global reads), verified by the same guarantees tests, so it is
drop-in behind the existing store seam.

Known caveat, documented deliberately: ``read_all`` orders by an IDENTITY
column, and under **concurrent writers** a smaller id can become visible after
a larger one has been consumed (commit-order vs id-order skew), which a purely
monotonic offset would then skip. The current deployment scope is a single
writer process, where ids are gap-free in consumption order. The production
fix when multi-writer arrives is a transactional outbox drained in commit
order — see ROADMAP.
"""

from __future__ import annotations

import threading
import uuid

import psycopg
from psycopg.rows import dict_row

from cqrs import ConcurrencyError

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
"""


class PostgresEventStore:
    """Durable, append-only event log on PostgreSQL."""

    def __init__(self, conninfo: str) -> None:
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
        self._lock = threading.Lock()

    def close(self) -> None:
        self._conn.close()

    # -- write path ----------------------------------------------------------

    def append(self, stream: str, event: dict) -> int:
        """Append ``event`` to ``stream``; return its per-stream ``seq``.

        Optimistic concurrency: the seq is computed as MAX+1 and the
        UNIQUE(stream, seq) constraint turns a lost race into
        :class:`ConcurrencyError`, exactly like the SQLite tier.
        """
        if not isinstance(stream, str) or not stream:
            raise ValueError("stream must be a non-empty string")
        if not isinstance(event, dict):
            raise TypeError("event must be a dict")

        event_id = event.get("event_id") or str(uuid.uuid4())
        with self._lock:
            try:
                with self._conn.transaction():
                    row = self._conn.execute(
                        "SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq "
                        "FROM events WHERE stream = %s",
                        (stream,),
                    ).fetchone()
                    assert row is not None
                    seq = int(row["next_seq"])
                    self._conn.execute(
                        "INSERT INTO events "
                        "(event_id, stream, seq, type, account_id, amount) "
                        "VALUES (%s, %s, %s, %s, %s, %s)",
                        (
                            event_id,
                            stream,
                            seq,
                            event.get("type"),
                            event.get("account_id"),
                            event.get("amount"),
                        ),
                    )
            except psycopg.errors.UniqueViolation as exc:
                # UNIQUE(stream, seq) => concurrent writer took our seq.
                # UNIQUE(event_id)    => duplicate append of the same event.
                raise ConcurrencyError(str(exc)) from exc
            return seq

    # -- read path -------------------------------------------------------------

    def read(self, stream: str) -> list[dict]:
        """Return all events in ``stream`` in append (seq) order."""
        return self.read_after(stream, 0)

    def read_after(self, stream: str, after_seq: int) -> list[dict]:
        """Return events in ``stream`` with ``seq`` > ``after_seq``, in order."""
        if after_seq < 0:
            raise ValueError("after_seq must be non-negative")
        with self._lock:
            rows = self._conn.execute(
                "SELECT event_id, stream, seq, type, account_id, amount "
                "FROM events WHERE stream = %s AND seq > %s ORDER BY seq ASC",
                (stream, after_seq),
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def read_all(self, after_id: int = 0, limit: int | None = None) -> list[dict]:
        """Return events across all streams with ``id`` > ``after_id``.

        The consumer feed; each event carries its global ``id`` for the
        persisted offset. See the module docstring for the multi-writer
        visibility caveat.
        """
        sql = (
            "SELECT id, event_id, stream, seq, type, account_id, amount "
            "FROM events WHERE id > %s ORDER BY id ASC"
        )
        params: tuple = (after_id,)
        if limit is not None:
            sql += " LIMIT %s"
            params = (after_id, limit)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        events = []
        for row in rows:
            event = self._row_to_event(row)
            event["id"] = int(row["id"])
            events.append(event)
        return events

    @staticmethod
    def _row_to_event(row: dict) -> dict:
        event = {
            "event_id": row["event_id"],
            "stream": row["stream"],
            "seq": int(row["seq"]),
            "type": row["type"],
        }
        if row["account_id"] is not None:
            event["account_id"] = row["account_id"]
        if row["amount"] is not None:
            event["amount"] = int(row["amount"])
        return event


__all__ = ["PostgresEventStore"]
