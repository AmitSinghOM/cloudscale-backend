"""Bounded retry with capped exponential backoff and full jitter.

Pure, dependency-free primitive: the sleep function and the jitter source are
injected so tests are deterministic and instantaneous. Only errors listed in
``retryable_errors`` are retried; anything else propagates on the first
attempt. After the final attempt the last retryable error propagates unchanged
so callers can classify it (e.g. dead-letter the delivery that exhausted its
budget).
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

_ResultT = TypeVar("_ResultT")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Immutable retry budget: attempts and backoff shape.

    ``max_attempts`` counts the first call, so ``max_attempts=1`` disables
    retries. Delay before retry ``n`` (1-based) is
    ``jitter * min(max_delay_seconds, base_delay_seconds * 2**(n - 1))`` with
    ``jitter`` drawn uniformly from [0, 1) — capped exponential backoff with
    full jitter.
    """

    max_attempts: int = 3
    base_delay_seconds: float = 0.05
    max_delay_seconds: float = 2.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay_seconds < 0:
            raise ValueError("base_delay_seconds must be non-negative")
        if self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError("max_delay_seconds must be >= base_delay_seconds")

    def backoff_ceiling(self, retry_number: int) -> float:
        """Return the un-jittered delay cap before 1-based retry ``retry_number``."""
        if retry_number < 1:
            raise ValueError("retry_number is 1-based")
        return min(
            self.max_delay_seconds,
            self.base_delay_seconds * (2 ** (retry_number - 1)),
        )


def call_with_retry(
    operation: Callable[[], _ResultT],
    policy: RetryPolicy,
    *,
    retryable_errors: tuple[type[BaseException], ...],
    sleep: Callable[[float], None] = time.sleep,
    random_fn: Callable[[], float] = random.random,
) -> _ResultT:
    """Invoke ``operation`` under ``policy``; return its first successful result.

    Non-retryable errors propagate immediately. The last retryable error
    propagates unchanged once the attempt budget is exhausted.
    """
    if not retryable_errors:
        raise ValueError("retryable_errors must name at least one error type")

    for attempt in range(1, policy.max_attempts + 1):
        try:
            return operation()
        except retryable_errors:
            if attempt == policy.max_attempts:
                raise
            sleep(random_fn() * policy.backoff_ceiling(attempt))
    raise AssertionError("unreachable: loop returns or raises")  # pragma: no cover


__all__ = ["RetryPolicy", "call_with_retry"]
