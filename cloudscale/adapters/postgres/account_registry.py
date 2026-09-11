"""PostgreSQL realization of the account ownership registry — pooled.

``register`` is a single INSERT ... ON CONFLICT DO NOTHING RETURNING, so a
race between two callers for the same account id is decided by the PRIMARY
KEY and the loser learns who won from the follow-up lookup.
"""

from __future__ import annotations

from datetime import UTC, datetime

from cloudscale.adapters.postgres import schema
from cloudscale.adapters.postgres.pool import ensure_schema, open_pool
from cloudscale.application.ports import RegistrationOutcome
from cloudscale.domain.commands import _validate_account_id

_SCHEMA = schema.ACCOUNTS


class PostgresAccountRegistry:
    def __init__(self, conninfo: str, *, pool_max: int | None = None) -> None:
        self._pool = open_pool(conninfo, max_size=pool_max)
        ensure_schema(self._pool, _SCHEMA)

    def close(self) -> None:
        self._pool.close()

    def register(self, account_id: str, owner_subject: str) -> RegistrationOutcome:
        _validate_account_id(account_id)
        if not isinstance(owner_subject, str) or not owner_subject:
            raise ValueError("owner_subject must be a non-empty string")
        with self._pool.connection() as conn:
            inserted = conn.execute(
                "INSERT INTO accounts (account_id, owner_subject, created_at) "
                "VALUES (%s, %s, %s) ON CONFLICT (account_id) DO NOTHING "
                "RETURNING account_id",
                (account_id, owner_subject, datetime.now(UTC).isoformat()),
            ).fetchone()
            if inserted is not None:
                return RegistrationOutcome.CREATED
            row = conn.execute(
                "SELECT owner_subject FROM accounts WHERE account_id = %s",
                (account_id,),
            ).fetchone()
        owner = str(row["owner_subject"]) if row else None
        return (
            RegistrationOutcome.ALREADY_OWNED_BY_CALLER
            if owner == owner_subject
            else RegistrationOutcome.TAKEN
        )

    def owner_of(self, account_id: str) -> str | None:
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT owner_subject FROM accounts WHERE account_id = %s",
                (account_id,),
            ).fetchone()
        return str(row["owner_subject"]) if row else None


__all__ = ["PostgresAccountRegistry"]
