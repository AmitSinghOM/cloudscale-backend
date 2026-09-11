"""Connection pooling shared by the PostgreSQL adapters.

Every adapter used to hold one connection behind a ``threading.Lock``; that
serialized all database work per process and capped the tier at one uvicorn
worker's worth of concurrency. Adapters now check a connection out of a
``psycopg_pool.ConnectionPool`` per operation. Connections are autocommit
(see the adapters for why) and use ``dict_row``.

``CLOUDSCALE_PG_POOL_MAX`` bounds each adapter's pool (default 4). With N
adapters per process, the process holds at most ``N * max`` connections.
"""

from __future__ import annotations

import os

from psycopg import Connection
from psycopg.rows import DictRow, dict_row
from psycopg_pool import ConnectionPool

SCHEMA_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtext('cloudscale_schema'))"

#: The concrete pool type every adapter holds; rows come back as dicts.
DictPool = ConnectionPool[Connection[DictRow]]


def open_pool(conninfo: str, *, max_size: int | None = None) -> DictPool:
    """Open a ready pool of autocommit, dict-row connections."""
    size = (
        max_size
        if max_size is not None
        else int(os.environ.get("CLOUDSCALE_PG_POOL_MAX", "4"))
    )
    if size < 1:
        raise ValueError("pool max_size must be at least 1")
    pool: DictPool = ConnectionPool(
        conninfo,
        connection_class=Connection[DictRow],
        min_size=1,
        max_size=size,
        kwargs={"autocommit": True, "row_factory": dict_row},
        open=True,
    )
    pool.wait(timeout=30.0)
    return pool


class SchemaNotMigratedError(RuntimeError):
    """Raised in ``migrations`` schema mode when the database is not at head."""


def schema_mode() -> str:
    mode = os.environ.get("CLOUDSCALE_PG_SCHEMA", "auto")
    if mode not in ("auto", "migrations"):
        raise ValueError("CLOUDSCALE_PG_SCHEMA must be 'auto' or 'migrations'")
    return mode


def ensure_schema(pool: DictPool, schema_sql: str) -> None:
    """Make the schema available according to ``CLOUDSCALE_PG_SCHEMA``.

    ``auto`` (default, dev/test): run the adapter's ``CREATE ... IF NOT
    EXISTS`` statements under the cross-process advisory lock.

    ``migrations`` (production): create NOTHING. Verify that Alembic has
    stamped the database at the revision this code requires and refuse to
    start otherwise - a deployment can never silently run against a schema
    it did not migrate.
    """
    if schema_mode() == "auto":
        with pool.connection() as conn, conn.transaction():
            conn.execute(SCHEMA_LOCK_SQL)
            conn.execute(schema_sql)
        return
    verify_migrated(pool)


def verify_migrated(pool: DictPool) -> None:
    from cloudscale.adapters.postgres.schema import CURRENT_REVISION

    with pool.connection() as conn:
        present = conn.execute(
            "SELECT to_regclass('alembic_version') IS NOT NULL AS present"
        ).fetchone()
        if not present or not present["present"]:
            raise SchemaNotMigratedError(
                "CLOUDSCALE_PG_SCHEMA=migrations but the database has no "
                "alembic_version table; run: python -m cloudscale.entrypoints.migrate"
            )
        row = conn.execute("SELECT version_num FROM alembic_version").fetchone()
    found = row["version_num"] if row else None
    if found != CURRENT_REVISION:
        raise SchemaNotMigratedError(
            f"database is at Alembic revision {found!r}; this build requires "
            f"{CURRENT_REVISION!r}; run: python -m cloudscale.entrypoints.migrate"
        )


__all__ = [
    "SCHEMA_LOCK_SQL",
    "SchemaNotMigratedError",
    "ensure_schema",
    "open_pool",
    "schema_mode",
    "verify_migrated",
]
