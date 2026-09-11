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


def ensure_schema(pool: DictPool, schema_sql: str) -> None:
    """Create tables under the cross-process advisory lock (startup races)."""
    with pool.connection() as conn, conn.transaction():
        conn.execute(SCHEMA_LOCK_SQL)
        conn.execute(schema_sql)


__all__ = ["SCHEMA_LOCK_SQL", "ensure_schema", "open_pool"]
