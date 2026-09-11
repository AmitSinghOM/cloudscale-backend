"""Shared, PostgreSQL-backed token-bucket rate limiter.

The in-process ``RateLimiter`` bounds one replica, so N replicas allow N×
the configured rate. This limiter keeps every subject's bucket in one
table and performs refill-and-take as ONE atomic upsert using the database
clock (``clock_timestamp()``), so all replicas draw from a single budget and
do not depend on each other's wall clocks.

Same interface as the in-process limiter: ``enabled`` and
``try_acquire(key) -> (allowed, retry_after_seconds)``.
"""

from __future__ import annotations

from cloudscale.adapters.postgres import schema
from cloudscale.adapters.postgres.pool import ensure_schema, open_pool

# One statement: insert a fresh bucket (capacity - 1) or refill the existing
# one and take a token — but only if at least one token is available after
# refill. RETURNING tells us whether we got in.
_ACQUIRE_SQL = """
INSERT INTO rate_limit_buckets (bucket_key, tokens, updated_at)
VALUES (%(key)s, %(capacity)s - 1, EXTRACT(EPOCH FROM clock_timestamp()))
ON CONFLICT (bucket_key) DO UPDATE SET
    tokens = LEAST(
        %(capacity)s,
        rate_limit_buckets.tokens
        + (EXTRACT(EPOCH FROM clock_timestamp()) - rate_limit_buckets.updated_at)
          * %(refill)s
    ) - 1,
    updated_at = EXTRACT(EPOCH FROM clock_timestamp())
WHERE LEAST(
    %(capacity)s,
    rate_limit_buckets.tokens
    + (EXTRACT(EPOCH FROM clock_timestamp()) - rate_limit_buckets.updated_at)
      * %(refill)s
) >= 1
RETURNING tokens
"""

_PEEK_SQL = """
SELECT LEAST(
    %(capacity)s,
    tokens + (EXTRACT(EPOCH FROM clock_timestamp()) - updated_at) * %(refill)s
) AS tokens
FROM rate_limit_buckets WHERE bucket_key = %(key)s
"""


class PostgresRateLimiter:
    """Token bucket per key shared across every replica using the database."""

    def __init__(
        self, conninfo: str, per_minute: int, *, pool_max: int | None = None
    ) -> None:
        if per_minute < 0:
            raise ValueError("per_minute must be non-negative")
        self._capacity = float(per_minute)
        self._refill_per_second = per_minute / 60.0
        self._pool = open_pool(conninfo, max_size=pool_max)
        ensure_schema(self._pool, schema.RATE_LIMIT)

    def close(self) -> None:
        self._pool.close()

    @property
    def enabled(self) -> bool:
        return self._capacity > 0

    def try_acquire(self, key: str) -> tuple[bool, float]:
        if not self.enabled:
            return True, 0.0
        params = {
            "key": key,
            "capacity": self._capacity,
            "refill": self._refill_per_second,
        }
        with self._pool.connection() as conn:
            taken = conn.execute(_ACQUIRE_SQL, params).fetchone()
            if taken is not None:
                return True, 0.0
            peek = conn.execute(_PEEK_SQL, params).fetchone()
        tokens = float(peek["tokens"]) if peek else 0.0
        return False, max(0.0, (1.0 - tokens) / self._refill_per_second)


__all__ = ["PostgresRateLimiter"]
