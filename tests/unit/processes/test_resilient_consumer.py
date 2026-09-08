"""End-to-end tests: resilient consumer over the real durable SQLite log.

Covers the Phase 2 contracts: transient failures are retried to success,
poison events are dead-lettered exactly once and never wedge the log, retry
exhaustion parks the event, an open circuit halts consumption without data
loss, and a replayed poison event is absorbed as a duplicate.
"""

from __future__ import annotations

import sqlite3

import pytest

from cloudscale.adapters.sqlite_compat.dead_letter_store import (
    DeadLetteringProjectionStore,
)
from cloudscale.processes.resilient_consumer import ConsumerReport, ResilientConsumer
from cloudscale.resilience import CircuitBreaker, RetryPolicy
from cqrs import SqliteEventStore


class _ScriptedProjection(DeadLetteringProjectionStore):
    """Projection whose apply failures are scripted per event_id.

    ``failures`` maps event_id -> list of errors raised on successive apply
    calls (popped front to back); once the list is empty the apply succeeds.
    """

    def __init__(self) -> None:
        super().__init__(path=":memory:")
        self.failures: dict[str, list[BaseException]] = {}

    def apply(self, event: dict) -> bool:
        queued = self.failures.get(str(event.get("event_id")), [])
        if queued:
            raise queued.pop(0)
        return super().apply(event)


def _fill(store: SqliteEventStore, count: int, stream: str = "acct-1") -> list[str]:
    event_ids: list[str] = []
    for index in range(count):
        event_id = f"evt-{stream}-{index}"
        store.append(
            stream,
            {
                "event_id": event_id,
                "type": "Deposited",
                "account_id": stream,
                "amount": 10 + index,
            },
        )
        event_ids.append(event_id)
    return event_ids


@pytest.fixture()
def store() -> SqliteEventStore:
    return SqliteEventStore(":memory:")


def _consumer(
    store: SqliteEventStore,
    projection: DeadLetteringProjectionStore,
    **overrides: object,
) -> ResilientConsumer:
    defaults: dict = {
        "retry_policy": RetryPolicy(max_attempts=3, base_delay_seconds=0.0),
        "retryable_errors": (sqlite3.OperationalError,),
    }
    defaults.update(overrides)
    return ResilientConsumer(store, projection, **defaults)


def test_clean_log_applies_everything(store: SqliteEventStore) -> None:
    projection = _ScriptedProjection()
    _fill(store, 5)
    report = _consumer(store, projection).run()
    assert report == ConsumerReport(
        applied=5, duplicates=0, dead_lettered=0, halted=False, halt_reason=None
    )
    assert projection.balance("acct-1")["balance"] == sum(range(10, 15))
    assert projection.dead_letter_count() == 0


def test_transient_failures_are_retried_to_success(store: SqliteEventStore) -> None:
    projection = _ScriptedProjection()
    event_ids = _fill(store, 3)
    projection.failures[event_ids[1]] = [
        sqlite3.OperationalError("database is locked"),
        sqlite3.OperationalError("database is locked"),
    ]
    report = _consumer(store, projection).run()
    assert report.applied == 3
    assert report.dead_lettered == 0
    assert projection.dead_letter_count() == 0


def test_poison_event_is_dead_lettered_and_does_not_wedge_the_log(
    store: SqliteEventStore,
) -> None:
    projection = _ScriptedProjection()
    event_ids = _fill(store, 3)
    # Deterministic error: poison on first attempt, no retries spent.
    projection.failures[event_ids[1]] = [ValueError("malformed payload")]

    report = _consumer(store, projection).run()

    assert report.applied == 2
    assert report.dead_lettered == 1
    parked = projection.dead_letters()
    assert [entry["event_id"] for entry in parked] == [event_ids[1]]
    assert parked[0]["error_type"] == "ValueError"
    assert parked[0]["attempts"] == 1
    assert parked[0]["payload"]["account_id"] == "acct-1"
    # The log is fully drained: offset moved past the poison event.
    assert projection.last_id() == 3
    # Events 0 and 2 applied; event 1 skipped.
    assert projection.balance("acct-1")["balance"] == 10 + 12


def test_retry_exhaustion_parks_the_event_with_the_full_attempt_count(
    store: SqliteEventStore,
) -> None:
    projection = _ScriptedProjection()
    event_ids = _fill(store, 2)
    projection.failures[event_ids[0]] = [
        sqlite3.OperationalError("database is locked")
    ] * 10  # outlives the 3-attempt budget

    breaker = CircuitBreaker(
        failure_threshold=100,  # keep the circuit out of this scenario
        counted_errors=(sqlite3.OperationalError,),
    )
    report = _consumer(store, projection, breaker=breaker).run()

    assert report.applied == 1
    assert report.dead_lettered == 1
    parked = projection.dead_letters()
    assert parked[0]["event_id"] == event_ids[0]
    assert parked[0]["attempts"] == 3
    assert projection.last_id() == 2


def test_open_circuit_halts_without_advancing_or_parking(
    store: SqliteEventStore,
) -> None:
    projection = _ScriptedProjection()
    event_ids = _fill(store, 3)
    # Enough transient failures to trip a threshold-2 breaker inside one
    # 3-attempt retry cycle, and to keep failing afterwards.
    projection.failures[event_ids[0]] = [
        sqlite3.OperationalError("database is locked")
    ] * 10

    breaker = CircuitBreaker(
        failure_threshold=2,
        reset_timeout_seconds=3600.0,
        counted_errors=(sqlite3.OperationalError,),
    )
    report = _consumer(store, projection, breaker=breaker).run()

    assert report.halted is True
    assert report.halt_reason is not None and "circuit open" in report.halt_reason
    assert report.applied == 0
    assert report.dead_lettered == 0
    # Nothing consumed, nothing parked: the event will be re-delivered.
    assert projection.last_id() == 0
    assert projection.dead_letter_count() == 0

    # After recovery (failures cleared, circuit reset) the same consumer
    # drains the log completely — no data was lost.
    projection.failures.clear()
    recovered = _consumer(store, projection).run()
    assert recovered.applied == 3
    assert projection.last_id() == 3


def test_replayed_poison_event_is_absorbed_as_duplicate(
    store: SqliteEventStore,
) -> None:
    projection = _ScriptedProjection()
    event_ids = _fill(store, 2)
    projection.failures[event_ids[1]] = [ValueError("malformed payload")]

    first = _consumer(store, projection).run()
    assert first.dead_lettered == 1

    # Simulate at-least-once redelivery: replay the whole log from offset 0.
    # Both events are absorbed — one was applied, one was dead-lettered, and
    # each claimed its event_id in processed_events.
    all_events = store.read_all(0)
    assert [projection.apply(event) for event in all_events] == [False, False]

    # Direct dead_letter replay is also absorbed.
    assert (
        projection.dead_letter(all_events[1], ValueError("replayed"), attempts=1)
        is False
    )
    assert projection.dead_letter_count() == 1


def test_dead_letter_synthesizes_a_key_for_events_without_event_id() -> None:
    projection = DeadLetteringProjectionStore(path=":memory:")
    recorded = projection.dead_letter(
        {"id": 7, "type": "Broken"}, ValueError("no event_id"), attempts=1
    )
    assert recorded is True
    parked = projection.dead_letters()
    assert parked[0]["event_id"] == "missing-event-id:log-7"
    assert projection.last_id() == 7
    with pytest.raises(ValueError):
        projection.dead_letter({"id": 8}, ValueError("bad attempts"), attempts=0)
