"""SQLite realization of the account ownership registry.

One row per account: who registered it and when. Registration is a single
INSERT; the PRIMARY KEY turns a repeat into a lookup that distinguishes
"same owner, idempotent" from "someone else's account".
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import UTC, datetime

from cloudscale.application.ports import RegistrationOutcome
from cloudscale.domain.commands import _validate_account_id

_SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    account_id    TEXT PRIMARY KEY,
    owner_subject TEXT NOT NULL,
    created_at    TEXT NOT NULL
);
"""


class SqliteAccountRegistry:
    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        try:
            self._conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError:
            pass
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self._lock = threading.Lock()

    def close(self) -> None:
        self._conn.close()

    def register(self, account_id: str, owner_subject: str) -> RegistrationOutcome:
        _validate_account_id(account_id)
        if not isinstance(owner_subject, str) or not owner_subject:
            raise ValueError("owner_subject must be a non-empty string")
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO accounts (account_id, owner_subject, created_at) "
                    "VALUES (?, ?, ?)",
                    (account_id, owner_subject, datetime.now(UTC).isoformat()),
                )
                self._conn.commit()
                return RegistrationOutcome.CREATED
            except sqlite3.IntegrityError:
                self._conn.rollback()
                owner = self._owner_locked(account_id)
        return (
            RegistrationOutcome.ALREADY_OWNED_BY_CALLER
            if owner == owner_subject
            else RegistrationOutcome.TAKEN
        )

    def owner_of(self, account_id: str) -> str | None:
        with self._lock:
            return self._owner_locked(account_id)

    def _owner_locked(self, account_id: str) -> str | None:
        row = self._conn.execute(
            "SELECT owner_subject FROM accounts WHERE account_id = ?", (account_id,)
        ).fetchone()
        return str(row["owner_subject"]) if row else None


__all__ = ["SqliteAccountRegistry"]
