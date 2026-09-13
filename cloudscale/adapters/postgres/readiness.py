"""Readiness probe for the PostgreSQL tier.

``check()`` performs a real round-trip on the shared pool and, when the
deployment runs in ``CLOUDSCALE_PG_SCHEMA=migrations`` mode, re-verifies the
Alembic revision. Either failing raises; the HTTP layer maps that to 503 so
an orchestrator stops routing here and a mismatched rollout fails fast.
Bounded by a short statement timeout so a hung database cannot hang the probe.
"""

from __future__ import annotations

import time

from cloudscale.adapters.postgres.pool import (
    DictPool,
    open_pool,
    schema_mode,
    verify_migrated,
)
from cloudscale.adapters.postgres.schema import CURRENT_REVISION

__all__ = ["PostgresReadinessProbe"]

_PROBE_TIMEOUT_MS = 2_000


class PostgresReadinessProbe:
    def __init__(self, conninfo: str, *, pool: DictPool | None = None) -> None:
        self._pool = pool or open_pool(conninfo, max_size=1)
        self._owns_pool = pool is None

    def check(self) -> dict[str, object]:
        started = time.perf_counter()
        with self._pool.connection() as conn:
            conn.execute(f"SET LOCAL statement_timeout = {_PROBE_TIMEOUT_MS}")
            row = conn.execute("SELECT 1 AS ok").fetchone()
            if row is None or row["ok"] != 1:
                raise RuntimeError("storage round-trip returned no row")
        checks: dict[str, object] = {
            "storage": "ok",
            "storage_ms": round((time.perf_counter() - started) * 1000, 2),
            "schema_mode": schema_mode(),
        }
        if schema_mode() == "migrations":
            verify_migrated(self._pool)  # raises SchemaNotMigratedError
            checks["schema_revision"] = CURRENT_REVISION
        return checks

    def close(self) -> None:
        if self._owns_pool:
            self._pool.close()
