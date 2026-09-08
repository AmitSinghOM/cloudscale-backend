"""Resilient projection consumer: retries, circuit breaking, dead-lettering.

Composition order per delivery, innermost first:

    breaker.call(apply)  ->  call_with_retry(...)  ->  classify outcome

- The breaker wraps each individual apply attempt, so every attempt counts
  toward its failure threshold and an unhealthy read model trips it even
  across different events.
- Retry only covers errors listed as transient. A deterministic error
  (malformed event, contract violation) is poison: it goes to the DLQ on the
  first attempt.
- A transient error that survives the whole retry budget is treated as poison
  too — parked in the DLQ so the log never wedges.
- ``CircuitOpenError`` is neither: the event is NOT poison, the downstream is
  unhealthy. The consumer halts without advancing the offset, so the event is
  re-delivered intact on the next run. Nothing is lost, nothing is parked.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from functools import partial
from typing import Protocol

from cloudscale.adapters.sqlite_compat.dead_letter_store import (
    DeadLetteringProjectionStore,
)
from cloudscale.resilience import (
    CircuitBreaker,
    CircuitOpenError,
    RetryPolicy,
    call_with_retry,
)

#: Errors treated as transient (retried) by default: infrastructure-level
#: SQLite failures such as a locked or momentarily unavailable database.
DEFAULT_RETRYABLE_ERRORS: tuple[type[BaseException], ...] = (sqlite3.OperationalError,)


class EventFeed(Protocol):
    """A durable, totally ordered log the consumer polls (``SqliteEventStore``)."""

    def read_all(self, after_id: int = 0, limit: int | None = None) -> list[dict]: ...


@dataclass(frozen=True, slots=True)
class ConsumerReport:
    """Outcome of one ``ResilientConsumer.run`` invocation."""

    applied: int
    duplicates: int
    dead_lettered: int
    halted: bool
    halt_reason: str | None


class ResilientConsumer:
    """Poll a durable log and apply events with bounded failure handling."""

    def __init__(
        self,
        store: EventFeed,
        projection: DeadLetteringProjectionStore,
        *,
        retry_policy: RetryPolicy | None = None,
        breaker: CircuitBreaker | None = None,
        retryable_errors: tuple[type[BaseException], ...] = DEFAULT_RETRYABLE_ERRORS,
        batch: int = 100,
    ) -> None:
        if batch < 1:
            raise ValueError("batch must be at least 1")
        self._store = store
        self._projection = projection
        self._retry_policy = retry_policy or RetryPolicy()
        self._breaker = breaker or CircuitBreaker(counted_errors=retryable_errors)
        self._retryable_errors = retryable_errors
        self._batch = batch

    def _apply_under_breaker(self, event: dict) -> bool:
        """One breaker-guarded apply attempt for ``event``."""
        return self._breaker.call(partial(self._projection.apply, event))

    def run(self) -> ConsumerReport:
        """Consume until the log is drained or the circuit opens."""
        applied = 0
        duplicates = 0
        dead_lettered = 0

        while True:
            events = self._store.read_all(self._projection.last_id(), limit=self._batch)
            if not events:
                return ConsumerReport(
                    applied=applied,
                    duplicates=duplicates,
                    dead_lettered=dead_lettered,
                    halted=False,
                    halt_reason=None,
                )

            for event in events:
                try:
                    mutated = call_with_retry(
                        partial(self._apply_under_breaker, event),
                        self._retry_policy,
                        retryable_errors=self._retryable_errors,
                    )
                except CircuitOpenError as error:
                    # Downstream unhealthy — not the event's fault. Halt with
                    # the offset untouched so this event is re-delivered.
                    return ConsumerReport(
                        applied=applied,
                        duplicates=duplicates,
                        dead_lettered=dead_lettered,
                        halted=True,
                        halt_reason=str(error),
                    )
                except self._retryable_errors as error:
                    # Retry budget exhausted: park it, advance past it.
                    if self._projection.dead_letter(
                        event, error, attempts=self._retry_policy.max_attempts
                    ):
                        dead_lettered += 1
                    else:
                        duplicates += 1
                except Exception as error:  # deterministic failure -> poison
                    if self._projection.dead_letter(event, error, attempts=1):
                        dead_lettered += 1
                    else:
                        duplicates += 1
                else:
                    if mutated:
                        applied += 1
                    else:
                        duplicates += 1


__all__ = [
    "ConsumerReport",
    "DEFAULT_RETRYABLE_ERRORS",
    "EventFeed",
    "ResilientConsumer",
]
