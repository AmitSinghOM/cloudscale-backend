"""Idempotent projection consumer over the durable event log.

At-least-once delivery is the honest assumption for any real log (Kafka
included): a consumer can crash after applying an event but before committing
its offset, so on restart it re-reads and re-delivers events. This module turns
at-least-once *delivery* into exactly-once *effect* on the read model.

How: the read model, the consumer offset, and the set of already-applied event
ids all live in the SAME SQLite database as (or alongside) the log. Applying an
event is one transaction that (1) records the event_id in ``processed_events``
and (2) mutates the projection and advances the offset. A duplicate delivery
hits the UNIQUE constraint on ``processed_events`` and the whole transaction is
rolled back — the balance never double-counts.

This is the "idempotent consumer" bullet from the README, realized without
Kafka/Postgres.
"""

from __future__ import annotations

import sqlite3
import threading
from typing import Any, Dict, Iterable


_SCHEMA = """
CREATE TABLE IF NOT EXISTS balances (
    account_id TEXT PRIMARY KEY,
    balance    INTEGER NOT NULL DEFAULT 0,
    version    INTEGER NOT NULL DEFAULT 0
);

-- Dedupe ledger: one row per event we have already applied. The PRIMARY KEY is
-- the idempotency guard — re-applying the same event_id raises IntegrityError.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY
);

-- Single-row offset table: the global log id of the last event we applied.
CREATE TABLE IF NOT EXISTS consumer_offset (
    consumer TEXT PRIMARY KEY,
    last_id  INTEGER NOT NULL DEFAULT 0
);
"""


class IdempotentProjectionStore:
    """Durable balance read model updated by an idempotent consumer.

    Survives restart (state is in SQLite) and tolerates duplicate/replayed
    delivery (dedupe by event_id inside the apply transaction).
    """

    def __init__(self, path: str = ":memory:", consumer: str = "balances") -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        try:
            self._conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError:
            pass
        self._conn.executescript(_SCHEMA)
        self._conn.execute(
            "INSERT OR IGNORE INTO consumer_offset (consumer, last_id) VALUES (?, 0)",
            (consumer,),
        )
        self._conn.commit()
        self._consumer = consumer
        self._lock = threading.Lock()

    def close(self) -> None:
        self._conn.close()

    # -- consumer offset ----------------------------------------------------

    def last_id(self) -> int:
        """Return the global log id of the last event applied (0 if none)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT last_id FROM consumer_offset WHERE consumer = ?",
                (self._consumer,),
            ).fetchone()
        return int(row["last_id"]) if row else 0

    # -- apply --------------------------------------------------------------

    def apply(self, event: dict) -> bool:
        """Apply one event to the read model, exactly once.

        ``event`` must carry ``event_id`` and (from the durable log) a global
        ``id``. Returns True if the event mutated state, False if it was a
        duplicate that was safely skipped. The dedupe check, the balance
        update, and the offset advance all commit together, so a crash can
        never leave the offset ahead of the applied state (or vice versa).
        """
        event_id = event.get("event_id")
        if not event_id:
            raise ValueError("event requires an event_id for idempotent apply")
        log_id = int(event.get("id", 0))

        with self._lock:
            try:
                # 1. Claim the event id. Duplicate => IntegrityError => skip.
                self._conn.execute(
                    "INSERT INTO processed_events (event_id) VALUES (?)",
                    (event_id,),
                )

                # 2. Mutate the read model.
                self._apply_to_balance(event)

                # 3. Advance the offset (monotonic; never moves backward).
                self._conn.execute(
                    "UPDATE consumer_offset SET last_id = MAX(last_id, ?) "
                    "WHERE consumer = ?",
                    (log_id, self._consumer),
                )
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                # Duplicate delivery of an already-applied event. No effect.
                self._conn.rollback()
                # Still make sure the offset reflects that we have seen this id,
                # so we do not re-poll it forever.
                self._conn.execute(
                    "UPDATE consumer_offset SET last_id = MAX(last_id, ?) "
                    "WHERE consumer = ?",
                    (log_id, self._consumer),
                )
                self._conn.commit()
                return False

    def _apply_to_balance(self, event: dict) -> None:
        etype = event.get("type")
        account_id = event.get("account_id")
        amount = event.get("amount") or 0
        if account_id is None:
            return

        self._conn.execute(
            "INSERT OR IGNORE INTO balances (account_id, balance, version) "
            "VALUES (?, 0, 0)",
            (account_id,),
        )
        if etype == "Deposited":
            self._conn.execute(
                "UPDATE balances SET balance = balance + ?, version = version + 1 "
                "WHERE account_id = ?",
                (amount, account_id),
            )
        elif etype == "Withdrawn":
            self._conn.execute(
                "UPDATE balances SET balance = balance - ?, version = version + 1 "
                "WHERE account_id = ?",
                (amount, account_id),
            )
        else:
            # Unknown event type: count the version bump (we saw it) but leave
            # the balance untouched, matching BalanceProjection semantics.
            self._conn.execute(
                "UPDATE balances SET version = version + 1 WHERE account_id = ?",
                (account_id,),
            )

    # -- query --------------------------------------------------------------

    def balance(self, account_id: str) -> Dict:
        """Return the read model for ``account_id``."""
        with self._lock:
            row = self._conn.execute(
                "SELECT account_id, balance, version FROM balances WHERE account_id = ?",
                (account_id,),
            ).fetchone()
        if row is None:
            return {"account_id": account_id, "balance": 0, "version": 0}
        return {
            "account_id": row["account_id"],
            "balance": int(row["balance"]),
            "version": int(row["version"]),
        }


def run_consumer(
    store: Any, projection: IdempotentProjectionStore, batch: int = 100
) -> int:
    """Poll ``store`` from the projection's offset and apply new events.

    ``store`` is any durable log exposing ``read_all(after_id, limit)`` (i.e.
    ``SqliteEventStore``). Returns the number of events that actually mutated
    the read model (duplicates are skipped). Calling this repeatedly is safe:
    it resumes from the persisted offset, so a restart mid-batch re-delivers
    at-least-once and the idempotent ``apply`` absorbs the overlap.
    """
    applied = 0
    while True:
        events: Iterable[dict] = store.read_all(projection.last_id(), limit=batch)
        events = list(events)
        if not events:
            break
        for event in events:
            if projection.apply(event):
                applied += 1
    return applied
