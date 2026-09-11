"""Leader election for the projection consumer via a PostgreSQL advisory lock.

Exactly one consumer per ``consumer`` name may drain the log at a time.
Each candidate holds a DEDICATED connection and calls
``pg_try_advisory_lock`` — a *session-level* lock. The database releases it
the moment the holder's connection ends (crash, kill, network loss), so a
standby acquires within one poll interval, with no lease timeouts to tune
and no split-brain window: PostgreSQL itself is the arbiter.

``held`` re-checks liveness by pinging the lease connection; if that ping
fails the process has lost leadership and must stop draining until it
re-acquires.
"""

from __future__ import annotations

import psycopg


class NoLease:
    """Single-host tiers (SQLite) have one consumer by construction."""

    def try_acquire(self) -> bool:
        return True

    def held(self) -> bool:
        return True

    def release(self) -> None:
        return None

    def close(self) -> None:
        return None


class PostgresConsumerLease:
    def __init__(self, conninfo: str, consumer: str = "balances") -> None:
        self._conninfo = conninfo
        self._consumer = consumer
        self._conn: psycopg.Connection | None = None
        self._held = False

    @property
    def lock_key_sql(self) -> str:
        return f"hashtext('cloudscale_consumer:{self._consumer}')"

    def try_acquire(self) -> bool:
        """Attempt leadership; non-blocking. Idempotent while held."""
        if self._held and self.held():
            return True
        self._held = False
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._conninfo, autocommit=True)
        row = self._conn.execute(
            f"SELECT pg_try_advisory_lock({self.lock_key_sql}) AS got"
        ).fetchone()
        self._held = bool(row and row[0])
        return self._held

    def held(self) -> bool:
        """Leadership is only as alive as the connection that holds the lock."""
        if not self._held or self._conn is None or self._conn.closed:
            self._held = False
            return False
        try:
            self._conn.execute("SELECT 1").fetchone()
        except psycopg.Error:
            self._held = False
            self._conn = None
            return False
        return True

    def release(self) -> None:
        if self._conn is not None and not self._conn.closed and self._held:
            try:
                self._conn.execute(f"SELECT pg_advisory_unlock({self.lock_key_sql})")
            except psycopg.Error:
                pass
        self._held = False

    def close(self) -> None:
        self.release()
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = None


__all__ = ["NoLease", "PostgresConsumerLease"]
