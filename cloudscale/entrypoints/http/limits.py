"""Abuse controls: per-subject rate limiting and request body size limits.

The rate limiter is an in-process token bucket keyed by authenticated
subject. Scope is stated honestly: it bounds a single replica, so with N
replicas the effective limit is N× — a shared limiter (Redis or the
database) is the multi-replica follow-up in ROADMAP Phase 5. The body
limit rejects oversized requests before the body is read.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response


class RateLimiter:
    """Token bucket per key: ``per_minute`` capacity, refilled continuously."""

    def __init__(
        self,
        per_minute: int,
        clock: Callable[[], float] = time.monotonic,
        max_keys: int = 100_000,
    ) -> None:
        if per_minute < 0:
            raise ValueError("per_minute must be non-negative")
        self._capacity = float(per_minute)
        self._refill_per_second = per_minute / 60.0
        self._clock = clock
        self._max_keys = max_keys
        self._buckets: dict[str, tuple[float, float]] = {}
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return self._capacity > 0

    def try_acquire(self, key: str) -> tuple[bool, float]:
        """Return ``(allowed, retry_after_seconds)`` for one request."""
        if not self.enabled:
            return True, 0.0
        now = self._clock()
        with self._lock:
            tokens, updated = self._buckets.get(key, (self._capacity, now))
            tokens = min(
                self._capacity, tokens + (now - updated) * self._refill_per_second
            )
            if tokens >= 1.0:
                self._buckets[key] = (tokens - 1.0, now)
                self._evict_if_needed()
                return True, 0.0
            self._buckets[key] = (tokens, now)
            return False, (1.0 - tokens) / self._refill_per_second

    def _evict_if_needed(self) -> None:
        # Bound memory under key churn: drop the stalest entries.
        if len(self._buckets) <= self._max_keys:
            return
        for key in sorted(self._buckets, key=lambda k: self._buckets[k][1])[
            : len(self._buckets) // 10
        ]:
            del self._buckets[key]


class BodySizeLimitMiddleware(BaseHTTPMiddleware):
    """Reject requests whose declared or streamed body exceeds ``max_bytes``."""

    def __init__(self, app: Any, max_bytes: int) -> None:
        super().__init__(app)
        self._max_bytes = max_bytes

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        declared = request.headers.get("content-length")
        if declared is not None:
            try:
                if int(declared) > self._max_bytes:
                    return _too_large(self._max_bytes)
            except ValueError:
                return JSONResponse(
                    status_code=400, content={"detail": "invalid Content-Length"}
                )
        elif request.method in ("POST", "PUT", "PATCH"):
            # Chunked body with no declared length: read up to the cap.
            body = await request.body()
            if len(body) > self._max_bytes:
                return _too_large(self._max_bytes)
        return await call_next(request)


def _too_large(max_bytes: int) -> JSONResponse:
    return JSONResponse(
        status_code=413,
        content={"detail": f"request body exceeds {max_bytes} bytes"},
    )


__all__ = ["BodySizeLimitMiddleware", "RateLimiter"]
