"""Consumer HA: leader lease semantics and the standby/leader drain loop."""

from __future__ import annotations

import os
import sqlite3
import uuid
from itertools import count

import pytest

from cloudscale.adapters.sqlite_compat.dead_letter_store import (
    DeadLetteringProjectionStore,
)
from cloudscale.entrypoints.consumer_loop import ConsumerMetrics, run_forever
from cqrs import SqliteEventStore


class _FakeLease:
    """Scripted leadership: ``grants[i]`` is the answer to the i-th acquire."""

    def __init__(self, grants: list[bool]) -> None:
        self._grants = iter(grants)
        self.acquires = 0
        self.released = False

    def try_acquire(self) -> bool:
        self.acquires += 1
        return next(self._grants, False)

    def held(self) -> bool:
        return True

    def release(self) -> None:
        self.released = True

    def close(self) -> None:
        self.release()


def _stack(tmp_path):
    feed = SqliteEventStore(str(tmp_path / "log.db"))
    projection = DeadLetteringProjectionStore(path=str(tmp_path / "p.db"))
    for amount in (10, 20, 30):
        feed.append(
            "account-a",
            {
                "event_id": str(uuid.uuid4()),
                "type": "Deposited",
                "account_id": "a",
                "amount": amount,
            },
        )
    return feed, projection


def _run(feed, projection, lease, passes: int, metrics=None) -> None:
    remaining = count(passes, -1)
    run_forever(
        feed,
        projection,
        lease,
        retryable_errors=(sqlite3.OperationalError,),
        poll_interval=0.0,
        should_continue=lambda: next(remaining) > 0,
        metrics=metrics,
        sleep=lambda _: None,
    )


def test_standby_never_drains(tmp_path) -> None:
    feed, projection = _stack(tmp_path)
    lease = _FakeLease([False, False, False])
    try:
        _run(feed, projection, lease, passes=3)
        assert lease.acquires == 3
        assert projection.last_id() == 0
        assert projection.balance("a")["balance"] == 0
    finally:
        projection.close()
        feed.close()


def test_leader_drains_and_reports_zero_lag(tmp_path) -> None:
    feed, projection = _stack(tmp_path)
    lease = _FakeLease([True, True])
    metrics = ConsumerMetrics(f"test-{uuid.uuid4().hex[:6]}")
    try:
        _run(feed, projection, lease, passes=2, metrics=metrics)
        assert projection.balance("a")["balance"] == 60
        assert metrics.is_leader._value.get() == 1
        assert metrics.lag_events._value.get() == 0
        assert metrics.applied._value.get() == 3
        assert metrics.last_drain_timestamp._value.get() > 0
    finally:
        projection.close()
        feed.close()


def test_losing_the_lease_stops_draining(tmp_path) -> None:
    """Leader for one pass, then loses the lock: new events must NOT be applied."""
    feed, projection = _stack(tmp_path)
    lease = _FakeLease([True, False, False])
    metrics = ConsumerMetrics(f"test-{uuid.uuid4().hex[:6]}")
    try:
        _run(feed, projection, lease, passes=1, metrics=metrics)
        assert projection.balance("a")["balance"] == 60
        feed.append(
            "account-a",
            {
                "event_id": str(uuid.uuid4()),
                "type": "Deposited",
                "account_id": "a",
                "amount": 100,
            },
        )
        _run(feed, projection, lease, passes=2, metrics=metrics)
        assert projection.balance("a")["balance"] == 60  # standby: untouched
        assert metrics.is_leader._value.get() == 0
    finally:
        projection.close()
        feed.close()


def test_head_id_tracks_the_log(tmp_path) -> None:
    feed, projection = _stack(tmp_path)
    try:
        assert feed.head_id() == 3
        assert SqliteEventStore(":memory:").head_id() == 0
    finally:
        projection.close()
        feed.close()


# -- PostgreSQL lease ---------------------------------------------------------------

psycopg = pytest.importorskip("psycopg")
_ADMIN_DSN = os.environ.get("CLOUDSCALE_TEST_PG", "postgresql://localhost/postgres")


def _pg_available() -> bool:
    try:
        with psycopg.connect(_ADMIN_DSN, connect_timeout=2):
            return True
    except psycopg.OperationalError:
        return False


@pytest.mark.skipif(not _pg_available(), reason="no PostgreSQL reachable")
def test_pg_lease_is_exclusive_and_fails_over_when_holder_dies() -> None:
    from cloudscale.adapters.postgres.consumer_lease import PostgresConsumerLease

    name = f"ha-{uuid.uuid4().hex[:8]}"
    leader = PostgresConsumerLease(_ADMIN_DSN, consumer=name)
    standby = PostgresConsumerLease(_ADMIN_DSN, consumer=name)
    other = PostgresConsumerLease(_ADMIN_DSN, consumer=f"{name}-other")
    try:
        assert leader.try_acquire() is True
        assert leader.try_acquire() is True  # idempotent while held
        assert standby.try_acquire() is False  # exclusive per consumer name
        assert other.try_acquire() is True  # different consumer, own lock

        # Simulate the leader process dying: its connection closes, the
        # session-level lock is released by PostgreSQL, standby takes over.
        leader._conn.close()  # type: ignore[union-attr]
        assert leader.held() is False
        assert standby.try_acquire() is True
        assert standby.held() is True
    finally:
        for lease in (leader, standby, other):
            lease.close()
