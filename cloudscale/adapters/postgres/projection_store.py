"""PostgreSQL projection store: idempotent apply, DLQ, and redrive.

Production realization of the read-model tier with the exact transaction
shape the SQLite tier proved out: the dedupe claim (``processed_events``
PRIMARY KEY), the balance mutation, and the offset advance commit together,
so at-least-once delivery yields exactly-once effect. Dead-lettering and
redrive reuse the same claim, so poison events never wedge the log and
replays are absorbed as duplicates.
"""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime

import psycopg
from psycopg.rows import dict_row

from cloudscale.adapters.compat import adapt_legacy_event
from cloudscale.adapters.sqlite_compat.dead_letter_store import RedriveOutcome

_SCHEMA = """
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

PRODUCTION_TIER_METADATA: dict[str, object] = {
    "environment": "production-capable",
    "storage_tier": "postgresql",
    "production": True,
}


class PostgresProjectionStore:
    """Durable balance read model with exactly-once apply and a DLQ."""

    def __init__(self, conninfo: str, consumer: str = "balances") -> None:
        self._conn = psycopg.connect(conninfo, row_factory=dict_row)
        with self._conn.transaction():
            self._conn.execute(_SCHEMA)
            self._conn.execute(
                "INSERT INTO consumer_offset (consumer, last_id) VALUES (%s, 0) "
                "ON CONFLICT (consumer) DO NOTHING",
                (consumer,),
            )
        self._consumer = consumer
        self._lock = threading.Lock()

    def close(self) -> None:
        self._conn.close()

    # -- consumer offset ------------------------------------------------------

    def last_id(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT last_id FROM consumer_offset WHERE consumer = %s",
                (self._consumer,),
            ).fetchone()
        return int(row["last_id"]) if row else 0

    # -- apply ------------------------------------------------------------------

    def apply(self, event: dict) -> bool:
        """Apply one event exactly once; return False for absorbed duplicates."""
        event_id = event.get("event_id")
        if not event_id:
            raise ValueError("event requires an event_id for idempotent apply")
        validated = adapt_legacy_event(event)
        log_id = int(event.get("id", 0))

        with self._lock:
            try:
                with self._conn.transaction():
                    self._conn.execute(
                        "INSERT INTO processed_events (event_id) VALUES (%s)",
                        (event_id,),
                    )
                    self._apply_to_balance(validated)
                    self._advance_offset(log_id)
                return True
            except psycopg.errors.UniqueViolation:
                with self._conn.transaction():
                    self._advance_offset(log_id)
                return False

    def _apply_to_balance(self, event: dict) -> None:
        account_id = event.get("account_id")
        if account_id is None:
            return
        amount = int(event.get("amount") or 0)
        event_type = event.get("type")
        delta = (
            amount
            if event_type == "Deposited"
            else -amount
            if event_type == "Withdrawn"
            else 0
        )
        self._conn.execute(
            "INSERT INTO balances (account_id, balance, version) "
            "VALUES (%s, %s, 1) "
            "ON CONFLICT (account_id) DO UPDATE SET "
            "balance = balances.balance + EXCLUDED.balance, "
            "version = balances.version + 1",
            (account_id, delta),
        )

    def _advance_offset(self, log_id: int) -> None:
        self._conn.execute(
            "UPDATE consumer_offset SET last_id = GREATEST(last_id, %s) "
            "WHERE consumer = %s",
            (log_id, self._consumer),
        )

    # -- dead letters ------------------------------------------------------------

    def dead_letter(self, event: dict, error: BaseException, attempts: int) -> bool:
        """Park ``event`` and advance past it, exactly once (same as SQLite)."""
        if attempts < 1:
            raise ValueError("attempts must be at least 1")
        log_id = int(event.get("id", 0))
        event_id = str(event.get("event_id") or f"missing-event-id:log-{log_id}")

        with self._lock:
            try:
                with self._conn.transaction():
                    self._conn.execute(
                        "INSERT INTO processed_events (event_id) VALUES (%s)",
                        (event_id,),
                    )
                    self._conn.execute(
                        "INSERT INTO dead_letters (event_id, log_id, payload, "
                        "error_type, error_message, attempts, dead_lettered_at) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
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
                return True
            except psycopg.errors.UniqueViolation:
                with self._conn.transaction():
                    self._advance_offset(log_id)
                return False

    def dead_letters(self) -> list[dict]:
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
        return int(row["n"]) if row else 0

    def redrive(self, event_id: str) -> RedriveOutcome:
        """Re-apply one parked event and remove its letter, exactly once."""
        with self._lock:
            row = self._conn.execute(
                "SELECT payload FROM dead_letters WHERE event_id = %s",
                (event_id,),
            ).fetchone()
            if row is None:
                return RedriveOutcome.NOT_FOUND

            event = json.loads(row["payload"])
            try:
                with self._conn.transaction():
                    self._apply_to_balance(adapt_legacy_event(event))
                    self._conn.execute(
                        "DELETE FROM dead_letters WHERE event_id = %s", (event_id,)
                    )
                return RedriveOutcome.APPLIED
            except Exception as error:
                with self._conn.transaction():
                    self._conn.execute(
                        "UPDATE dead_letters SET attempts = attempts + 1, "
                        "error_type = %s, error_message = %s, "
                        "dead_lettered_at = %s WHERE event_id = %s",
                        (
                            type(error).__name__,
                            str(error),
                            datetime.now(UTC).isoformat(),
                            event_id,
                        ),
                    )
                return RedriveOutcome.FAILED_AGAIN

    def redrive_all(self) -> dict[str, RedriveOutcome]:
        event_ids = [entry["event_id"] for entry in self.dead_letters()]
        return {event_id: self.redrive(event_id) for event_id in event_ids}

    # -- query -----------------------------------------------------------------

    def balance(self, account_id: str) -> dict:
        with self._lock:
            row = self._conn.execute(
                "SELECT account_id, balance, version FROM balances "
                "WHERE account_id = %s",
                (account_id,),
            ).fetchone()
        if row is None:
            return {"account_id": account_id, "balance": 0, "version": 0}
        return {
            "account_id": row["account_id"],
            "balance": int(row["balance"]),
            "version": int(row["version"]),
        }

    # -- metadata ----------------------------------------------------------------

    @property
    def metadata(self) -> dict[str, object]:
        return dict(PRODUCTION_TIER_METADATA)


__all__ = ["PostgresProjectionStore", "PRODUCTION_TIER_METADATA"]
