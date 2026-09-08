"""Durable dead-letter queue sharing the projection's transaction boundary.

A poison message must not block the log forever, must not be silently
dropped, and must not be double-recorded on replay. This store extends the
idempotent projection with a ``dead_letters`` table in the SAME SQLite
database: dead-lettering an event claims its ``event_id`` in
``processed_events``, inserts the dead-letter row, and advances the consumer
offset in ONE transaction. A crash between delivery and commit re-delivers
the event; the ``processed_events`` PRIMARY KEY then absorbs the duplicate —
the same exactly-once mechanism the happy path uses.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime

from cloudscale.adapters.sqlite_compat.projection_store import (
    IdempotentProjectionStore,
)

_DLQ_SCHEMA = """
CREATE TABLE IF NOT EXISTS dead_letters (
    event_id         TEXT PRIMARY KEY,
    log_id           INTEGER NOT NULL,
    payload          TEXT NOT NULL,
    error_type       TEXT NOT NULL,
    error_message    TEXT NOT NULL,
    attempts         INTEGER NOT NULL,
    dead_lettered_at TEXT NOT NULL
);
"""


class DeadLetteringProjectionStore(IdempotentProjectionStore):
    """Idempotent projection store that can park poison events durably."""

    def __init__(self, path: str = ":memory:", consumer: str = "balances") -> None:
        super().__init__(path=path, consumer=consumer)
        self._conn.executescript(_DLQ_SCHEMA)
        self._conn.commit()

    def dead_letter(self, event: dict, error: BaseException, attempts: int) -> bool:
        """Park ``event`` in the DLQ and advance past it, exactly once.

        Returns True if the event was recorded, False if it was already
        processed or dead-lettered (a replayed duplicate). Either way the
        consumer offset advances so the poison event never wedges the log.
        """
        if attempts < 1:
            raise ValueError("attempts must be at least 1")
        log_id = int(event.get("id", 0))
        event_id = str(event.get("event_id") or f"missing-event-id:log-{log_id}")

        with self._lock:
            try:
                # Claim the event id with the same guard the happy path uses,
                # so replays of a dead-lettered event are absorbed as duplicates.
                self._conn.execute(
                    "INSERT INTO processed_events (event_id) VALUES (?)",
                    (event_id,),
                )
                self._conn.execute(
                    "INSERT INTO dead_letters (event_id, log_id, payload, "
                    "error_type, error_message, attempts, dead_lettered_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        event_id,
                        log_id,
                        json.dumps(event, sort_keys=True, default=repr),
                        type(error).__name__,
                        str(error),
                        attempts,
                        datetime.now(UTC).isoformat(),
                    ),
                )
                self._advance_offset(log_id)
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                # Already processed or already dead-lettered.
                self._conn.rollback()
                self._advance_offset(log_id)
                self._conn.commit()
                return False

    def dead_letters(self) -> list[dict]:
        """Return all parked events, oldest first, payloads decoded."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT event_id, log_id, payload, error_type, error_message, "
                "attempts, dead_lettered_at FROM dead_letters ORDER BY log_id ASC"
            ).fetchall()
        return [
            {
                "event_id": row["event_id"],
                "log_id": int(row["log_id"]),
                "payload": json.loads(row["payload"]),
                "error_type": row["error_type"],
                "error_message": row["error_message"],
                "attempts": int(row["attempts"]),
                "dead_lettered_at": row["dead_lettered_at"],
            }
            for row in rows
        ]

    def dead_letter_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM dead_letters"
            ).fetchone()
        return int(row["n"])

    def _advance_offset(self, log_id: int) -> None:
        """Advance the offset monotonically; caller holds the lock and commits."""
        self._conn.execute(
            "UPDATE consumer_offset SET last_id = MAX(last_id, ?) WHERE consumer = ?",
            (log_id, self._consumer),
        )


__all__ = ["DeadLetteringProjectionStore"]
