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
from typing import Any, Protocol

from starlette.responses import JSONResponse


class RateLimiterLike(Protocol):
    """What the app needs from any limiter: in-process or shared."""

    @property
    def enabled(self) -> bool: ...

    def try_acquire(self, key: str) -> tuple[bool, float]: ...


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


class BodySizeLimitMiddleware:
    """Reject requests whose declared or streamed body exceeds ``max_bytes``.

    Pure ASGI (no ``BaseHTTPMiddleware`` re-streaming cost): a declared
    Content-Length over the cap is rejected before any body byte is read; an
    undeclared (chunked) body is counted as it streams and cut off with 413
    the moment it passes the cap.
    """

    def __init__(self, app: Any, max_bytes: int) -> None:
        self._app = app
        self._max_bytes = max_bytes

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        declared = None
        for name, value in scope.get("headers", ()):
            if name == b"content-length":
                declared = value
                break
        if declared is not None:
            try:
                too_large = int(declared) > self._max_bytes
            except ValueError:
                response = JSONResponse(
                    status_code=400, content={"detail": "invalid Content-Length"}
                )
                await response(scope, receive, send)
                return
            if too_large:
                await _too_large(self._max_bytes)(scope, receive, send)
                return
            await self._app(scope, receive, send)
            return

        seen = 0

        async def counting_receive() -> dict:
            nonlocal seen
            message = await receive()
            if message["type"] == "http.request":
                seen += len(message.get("body", b""))
                if seen > self._max_bytes:
                    raise _BodyTooLarge()
            return message

        try:
            await self._app(scope, counting_receive, send)
        except _BodyTooLarge:
            await _too_large(self._max_bytes)(scope, receive, send)


class _BodyTooLarge(Exception):
    """Internal signal: a streamed body exceeded the cap."""


def _too_large(max_bytes: int) -> JSONResponse:
    return JSONResponse(
        status_code=413,
        content={"detail": f"request body exceeds {max_bytes} bytes"},
    )


__all__ = ["BodySizeLimitMiddleware", "RateLimiter", "RateLimiterLike"]
