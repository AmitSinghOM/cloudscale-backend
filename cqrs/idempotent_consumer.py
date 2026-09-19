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

from cloudscale.application.transfers_read_model import (
    kind_for_leg_count,
    transfer_row_effect,
)
from cloudscale.domain.events import BALANCE_SIGN, HELD_SIGN


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

-- Per-hold read model (ADR-0014): the sweeper reads open holds past expiry.
CREATE TABLE IF NOT EXISTS holds (
    hold_id    TEXT PRIMARY KEY,
    source     TEXT NOT NULL,
    target     TEXT NOT NULL,
    amount     INTEGER NOT NULL,
    expires_at TEXT NOT NULL,
    state      TEXT NOT NULL
);

-- Per-set read model (ADR-0015): "was this payment reverted, by what?"
CREATE TABLE IF NOT EXISTS transfers (
    transfer_id TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,
    reverts     TEXT,
    reverted_by TEXT
);
CREATE TABLE IF NOT EXISTS transfer_legs (
    transfer_id TEXT NOT NULL,
    account_id  TEXT NOT NULL,
    amount      INTEGER NOT NULL,
    direction   TEXT NOT NULL,
    PRIMARY KEY (transfer_id, account_id)
);
"""


def _add_held_if_missing(conn: sqlite3.Connection) -> None:
    """In-place upgrade for projection files created before holds (ADR-0014)."""
    columns = {row[1] for row in conn.execute("PRAGMA table_info(balances)")}
    if "held" not in columns:
        conn.execute("ALTER TABLE balances ADD COLUMN held INTEGER NOT NULL DEFAULT 0")


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
        _add_held_if_missing(self._conn)
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
        # Unknown event type: count the version bump (we saw it) but leave the
        # balance untouched (delta 0), matching BalanceProjection semantics.
        delta = BALANCE_SIGN.get(str(etype), 0) * amount
        held_delta = HELD_SIGN.get(str(etype), 0) * amount
        self._conn.execute(
            "UPDATE balances SET balance = balance + ?, held = held + ?, "
            "version = version + 1 WHERE account_id = ?",
            (delta, held_delta, account_id),
        )
        self._apply_to_holds(str(etype), event)
        self._apply_to_transfers(event)

    def _apply_to_transfers(self, event: dict) -> None:
        """Maintain the per-set read model (ADR-0015); every statement is idempotent."""
        effect = transfer_row_effect(event)
        if effect is None:
            return
        self._conn.execute(
            "INSERT OR IGNORE INTO transfers (transfer_id, kind, reverts) VALUES (?, ?, ?)",
            (effect.transfer_id, effect.kind, effect.reverts),
        )
        self._conn.execute(
            "INSERT OR IGNORE INTO transfer_legs "
            "(transfer_id, account_id, amount, direction) VALUES (?, ?, ?, ?)",
            (effect.transfer_id, effect.account_id, effect.amount, effect.direction),
        )
        if effect.marks_reverted is not None:
            self._conn.execute(
                "UPDATE transfers SET reverted_by = ? "
                "WHERE transfer_id = ? AND reverted_by IS NULL",
                (effect.transfer_id, effect.marks_reverted),
            )

    def _apply_to_holds(self, etype: str, event: dict) -> None:
        """Maintain the per-hold read model (ADR-0014)."""
        hold_id = event.get("transfer_id")
        if hold_id is None:
            return
        if etype == "HoldPlaced":
            self._conn.execute(
                "INSERT OR IGNORE INTO holds "
                "(hold_id, source, target, amount, expires_at, state) "
                "VALUES (?, ?, ?, ?, ?, 'open')",
                (
                    hold_id,
                    event["account_id"],
                    event["counterparty"],
                    int(event["amount"]),
                    event["expires_at"],
                ),
            )
        elif etype == "HoldPosted":
            self._conn.execute(
                "UPDATE holds SET state = 'posted' WHERE hold_id = ?", (hold_id,)
            )
        elif etype == "HoldReleased" and event.get("release_reason") != "partial":
            self._conn.execute(
                "UPDATE holds SET state = ? WHERE hold_id = ?",
                (event.get("release_reason"), hold_id),
            )

    # -- query --------------------------------------------------------------

    def balance(self, account_id: str) -> Dict:
        """Return the read model for ``account_id``."""
        with self._lock:
            row = self._conn.execute(
                "SELECT account_id, balance, held, version FROM balances "
                "WHERE account_id = ?",
                (account_id,),
            ).fetchone()
        if row is None:
            return {"account_id": account_id, "balance": 0, "held": 0, "version": 0}
        return {
            "account_id": row["account_id"],
            "balance": int(row["balance"]),
            "held": int(row["held"]),
            "version": int(row["version"]),
        }

    def transfer(self, transfer_id: str) -> Dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT transfer_id, kind, reverts, reverted_by FROM transfers "
                "WHERE transfer_id = ?",
                (transfer_id,),
            ).fetchone()
            if row is None:
                return None
            legs = self._conn.execute(
                "SELECT account_id, amount, direction FROM transfer_legs "
                "WHERE transfer_id = ? ORDER BY account_id",
                (transfer_id,),
            ).fetchall()
        out = dict(row)
        out["legs"] = [dict(leg) for leg in legs]
        out["kind"] = kind_for_leg_count(out["kind"], len(legs))
        return out

    def open_holds_expired_at(self, now_iso: str) -> list[Dict]:
        """Open holds whose ``expires_at`` <= ``now_iso`` (the sweeper's query)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT hold_id, source, target, amount, expires_at FROM holds "
                "WHERE state = 'open' AND expires_at <= ? ORDER BY expires_at ASC",
                (now_iso,),
            ).fetchall()
        return [dict(row) for row in rows]


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
