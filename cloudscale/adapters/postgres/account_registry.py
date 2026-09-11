"""PostgreSQL realization of the account ownership registry.

Autocommit connection (see the other PG adapters for why); ``register`` is a
single INSERT ... ON CONFLICT DO NOTHING RETURNING, so a race between two
callers for the same account id is decided by the PRIMARY KEY and the loser
learns who won from the follow-up lookup.
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime

import psycopg
from psycopg.rows import dict_row

from cloudscale.application.ports import RegistrationOutcome
from cloudscale.domain.commands import _validate_account_id

_SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    account_id    TEXT PRIMARY KEY,
    owner_subject TEXT NOT NULL,
    created_at    TEXT NOT NULL
);
"""


class PostgresAccountRegistry:
    def __init__(self, conninfo: str) -> None:
        self._conn = psycopg.connect(conninfo, row_factory=dict_row, autocommit=True)
        with self._conn.transaction():
            self._conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext('cloudscale_schema'))"
            )
            self._conn.execute(_SCHEMA)
        self._lock = threading.Lock()

    def close(self) -> None:
        self._conn.close()

    def register(self, account_id: str, owner_subject: str) -> RegistrationOutcome:
        _validate_account_id(account_id)
        if not isinstance(owner_subject, str) or not owner_subject:
            raise ValueError("owner_subject must be a non-empty string")
        with self._lock:
            inserted = self._conn.execute(
                "INSERT INTO accounts (account_id, owner_subject, created_at) "
                "VALUES (%s, %s, %s) ON CONFLICT (account_id) DO NOTHING "
                "RETURNING account_id",
                (account_id, owner_subject, datetime.now(UTC).isoformat()),
            ).fetchone()
            if inserted is not None:
                return RegistrationOutcome.CREATED
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
            "SELECT owner_subject FROM accounts WHERE account_id = %s", (account_id,)
        ).fetchone()
        return str(row["owner_subject"]) if row else None


__all__ = ["PostgresAccountRegistry"]
