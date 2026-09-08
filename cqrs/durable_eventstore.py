"""Durable, append-only event log backed by SQLite.

Phase 1 makes the event log *durable*. The README/ROADMAP name Kafka + Postgres
as the eventual log + read-model store; those are deliberately deferred (heavy
deps, and this host is memory-tight). SQLite is the stdlib-only realization of
the same tier: it gives us an ACID append (durability across restart), a UNIQUE
constraint enforcing per-stream ordering, and a stable per-event id that makes
at-least-once consumers safe to dedupe against. See ``IdempotentProjectionStore``
for the exactly-once projection effect built on top of this log.

The seam is intentional: ``EventStore`` (the in-memory Phase-0 store) and
``SqliteEventStore`` implement the same minimal interface, so the command side
does not care which log it appends to.
"""

from __future__ import annotations

import sqlite3
import threading
import uuid
from typing import List, Optional


_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    -- Global, gap-tolerant total order across the whole log. Consumers use
    -- this as their offset ("log position"). AUTOINCREMENT guarantees ids are
    -- never reused even after deletes, so an offset always moves forward.
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id   TEXT NOT NULL,
    stream     TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    type       TEXT NOT NULL,
    account_id TEXT,
    amount     INTEGER,
    -- (stream, seq) is the per-stream ordering + optimistic-concurrency guard:
    -- two writers racing on the same seq collide here instead of corrupting
    -- the log.
    UNIQUE (stream, seq),
    -- A stable event identity. The store rejects a re-append of the same
    -- event_id, and consumers dedupe against it under at-least-once delivery.
    UNIQUE (event_id)
);
"""


class ConcurrencyError(RuntimeError):
    """Raised when an append loses the race for a per-stream sequence number."""


class SqliteEventStore:
    """Append-only event log persisted to SQLite.

    Durable: every ``append`` commits a transaction, so events survive a
    process restart when the same ``path`` is reopened. Ordered: each event
    gets a per-stream 1-based ``seq`` (matching the in-memory store) plus a
    global ``id`` giving a total order for consumers.
    """

    def __init__(self, path: str = ":memory:") -> None:
        # check_same_thread=False + our own lock: SQLite is safe for our
        # coarse, serialized access pattern and we want the store usable from
        # a consumer thread without a per-thread connection.
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        # WAL improves durability/concurrency characteristics for a real file;
        # it is a no-op / harmless for :memory:.
        try:
            self._conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError:
            pass
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self._lock = threading.Lock()

    def close(self) -> None:
        self._conn.close()

    # -- write path ---------------------------------------------------------

    def append(self, stream: str, event: dict) -> int:
        """Append ``event`` to ``stream``; return its per-stream ``seq``.

        The event is stamped with a stable ``event_id`` (if the caller did not
        supply one) and its ``seq`` before being committed, mirroring the
        in-memory store's contract. The append is a single transaction, so a
        crash either persists the whole event or none of it.
        """
        if not isinstance(stream, str) or not stream:
            raise ValueError("stream must be a non-empty string")
        if not isinstance(event, dict):
            raise TypeError("event must be a dict")

        event_id = event.get("event_id") or str(uuid.uuid4())

        with self._lock:
            cur = self._conn.execute(
                "SELECT COALESCE(MAX(seq), 0) AS maxseq FROM events WHERE stream = ?",
                (stream,),
            )
            seq = int(cur.fetchone()["maxseq"]) + 1
            try:
                self._conn.execute(
                    "INSERT INTO events (event_id, stream, seq, type, account_id, amount) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        event_id,
                        stream,
                        seq,
                        event.get("type"),
                        event.get("account_id"),
                        event.get("amount"),
                    ),
                )
                self._conn.commit()
            except sqlite3.IntegrityError as exc:
                self._conn.rollback()
                # UNIQUE(stream, seq) => concurrent writer took our seq.
                # UNIQUE(event_id)    => duplicate append of the same event.
                raise ConcurrencyError(str(exc)) from exc
            return seq

    # -- read path ----------------------------------------------------------

    def read(self, stream: str) -> List[dict]:
        """Return all events in ``stream`` in append (seq) order."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT event_id, stream, seq, type, account_id, amount "
                "FROM events WHERE stream = ? ORDER BY seq ASC",
                (stream,),
            ).fetchall()
        return [self._row_to_event(r) for r in rows]

    def read_after(self, stream: str, after_seq: int) -> List[dict]:
        """Return events in ``stream`` with ``seq`` > ``after_seq``, in order.

        ``read_after(stream, 0)`` is equivalent to ``read(stream)``. Backed by
        the UNIQUE(stream, seq) index, so the suffix read costs O(delta).
        """
        if after_seq < 0:
            raise ValueError("after_seq must be non-negative")
        with self._lock:
            rows = self._conn.execute(
                "SELECT event_id, stream, seq, type, account_id, amount "
                "FROM events WHERE stream = ? AND seq > ? ORDER BY seq ASC",
                (stream, after_seq),
            ).fetchall()
        return [self._row_to_event(r) for r in rows]

    def read_all(self, after_id: int = 0, limit: Optional[int] = None) -> List[dict]:
        """Return events across all streams with ``id`` > ``after_id``.

        This is the consumer feed: a global, totally-ordered stream of events
        the projection consumer polls. Each returned event carries its global
        ``id`` so the consumer can persist an offset.
        """
        sql = (
            "SELECT id, event_id, stream, seq, type, account_id, amount "
            "FROM events WHERE id > ? ORDER BY id ASC"
        )
        params: tuple = (after_id,)
        if limit is not None:
            sql += " LIMIT ?"
            params = (after_id, limit)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        out = []
        for r in rows:
            e = self._row_to_event(r)
            e["id"] = int(r["id"])
            out.append(e)
        return out

    @staticmethod
    def _row_to_event(r: sqlite3.Row) -> dict:
        e = {
            "event_id": r["event_id"],
            "stream": r["stream"],
            "seq": int(r["seq"]),
            "type": r["type"],
        }
        if r["account_id"] is not None:
            e["account_id"] = r["account_id"]
        if r["amount"] is not None:
            e["amount"] = int(r["amount"])
        return e
