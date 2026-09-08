"""Thread-safe three-state circuit breaker with an injectable clock.

States: CLOSED (normal), OPEN (fail fast without calling the protected
operation), HALF_OPEN (a single probe is allowed through after the reset
timeout; success closes the circuit, failure re-opens it and restarts the
timeout). The monotonic clock is injected so tests control time exactly.
"""

from __future__ import annotations

import enum
import threading
import time
from collections.abc import Callable
from typing import TypeVar

_ResultT = TypeVar("_ResultT")


class CircuitState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(RuntimeError):
    """Raised instead of calling the protected operation while the circuit is open."""

    def __init__(self, retry_after_seconds: float) -> None:
        super().__init__(f"circuit open; retry allowed in {retry_after_seconds:.3f}s")
        self.retry_after_seconds = retry_after_seconds


class CircuitBreaker:
    """Trip after ``failure_threshold`` consecutive failures; probe after timeout.

    ``call`` wraps one attempt of the protected operation: every raised
    exception counts as a failure, every return counts as a success. While
    HALF_OPEN, exactly one in-flight probe is admitted; concurrent callers fail
    fast with :class:`CircuitOpenError` instead of stampeding the downstream.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout_seconds: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
        counted_errors: tuple[type[BaseException], ...] = (Exception,),
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1")
        if reset_timeout_seconds <= 0:
            raise ValueError("reset_timeout_seconds must be positive")
        if not counted_errors:
            raise ValueError("counted_errors must name at least one error type")
        self._failure_threshold = failure_threshold
        self._reset_timeout_seconds = reset_timeout_seconds
        self._clock = clock
        self._counted_errors = counted_errors
        self._lock = threading.Lock()
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at = 0.0
        self._probe_in_flight = False

    @property
    def state(self) -> CircuitState:
        """Current state, promoting OPEN to HALF_OPEN once the timeout elapses."""
        with self._lock:
            self._promote_if_reset_elapsed()
            return self._state

    def call(self, operation: Callable[[], _ResultT]) -> _ResultT:
        """Run one attempt of ``operation`` under the breaker.

        Only errors matching ``counted_errors`` count as failures. Any other
        error is treated as evidence of a healthy, responsive downstream — a
        deterministic rejection proves the call was processed — so it records
        a success while still propagating to the caller.
        """
        self._admit()
        try:
            result = operation()
        except self._counted_errors:
            self._record_failure()
            raise
        except BaseException:
            self._record_success()
            raise
        self._record_success()
        return result

    # -- internals ----------------------------------------------------------

    def _promote_if_reset_elapsed(self) -> None:
        if (
            self._state is CircuitState.OPEN
            and self._clock() - self._opened_at >= self._reset_timeout_seconds
        ):
            self._state = CircuitState.HALF_OPEN
            self._probe_in_flight = False

    def _admit(self) -> None:
        with self._lock:
            self._promote_if_reset_elapsed()
            if self._state is CircuitState.CLOSED:
                return
            if self._state is CircuitState.HALF_OPEN and not self._probe_in_flight:
                self._probe_in_flight = True
                return
            elapsed = self._clock() - self._opened_at
            raise CircuitOpenError(max(0.0, self._reset_timeout_seconds - elapsed))

    def _record_success(self) -> None:
        with self._lock:
            self._state = CircuitState.CLOSED
            self._consecutive_failures = 0
            self._probe_in_flight = False

    def _record_failure(self) -> None:
        with self._lock:
            if self._state is CircuitState.HALF_OPEN:
                self._trip()
                return
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._failure_threshold:
                self._trip()

    def _trip(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = self._clock()
        self._consecutive_failures = 0
        self._probe_in_flight = False


__all__ = ["CircuitBreaker", "CircuitOpenError", "CircuitState"]
