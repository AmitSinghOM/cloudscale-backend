"""Continuously drain the durable log into the projection, as a real process.

    CLOUDSCALE_LOG_DB=... CLOUDSCALE_PROJECTION_DB=... \\
        python -m cloudscale.entrypoints.consumer_loop

Storage tier selection mirrors the HTTP server (``CLOUDSCALE_STORAGE``):
``sqlite`` (default) or ``postgres`` (``CLOUDSCALE_PG_DSN``).

High availability (postgres): run as many replicas as you like. Each takes a
PostgreSQL session-level advisory lock for its consumer name; exactly one
drains, the rest stand by. If the leader dies, its connection ends, the lock
drops, and a standby becomes leader within one poll interval — no lease
timeouts, no split brain. The SQLite tier is single-host by construction.

Metrics (``CLOUDSCALE_CONSUMER_METRICS_PORT``): Prometheus gauges for
leadership, projection lag in events, last-drain timestamp, and counters
for applied / dead-lettered events, served by a tiny HTTP endpoint.

Uses the Phase 2 resilient consumer per drain pass, so poison events are
dead-lettered and an open circuit pauses (without losing the offset) until
the next pass. Exits cleanly on SIGTERM/SIGINT.
"""

from __future__ import annotations

import logging
import os
import signal
import time
from collections.abc import Callable
from typing import Protocol

from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server

from cloudscale.processes.resilient_consumer import (
    DeadLetteringProjection,
    EventFeed,
    ResilientConsumer,
)

_POLL_INTERVAL_SECONDS = 0.02
LOGGER = logging.getLogger("cloudscale.consumer")


class _ClosableFeed(EventFeed, Protocol):
    def close(self) -> None: ...


class _ClosableProjection(DeadLetteringProjection, Protocol):
    def close(self) -> None: ...


class Lease(Protocol):
    def try_acquire(self) -> bool: ...

    def held(self) -> bool: ...

    def release(self) -> None: ...

    def close(self) -> None: ...


class ConsumerMetrics:
    """Prometheus instruments for one consumer process (own registry)."""

    def __init__(self, consumer: str) -> None:
        self.registry = CollectorRegistry()
        labels = {"consumer": consumer}

        def gauge(name: str, doc: str) -> Gauge:
            return Gauge(
                name, doc, labelnames=("consumer",), registry=self.registry
            ).labels(**labels)

        def counter(name: str, doc: str) -> Counter:
            return Counter(
                name, doc, labelnames=("consumer",), registry=self.registry
            ).labels(**labels)

        self.is_leader = gauge(
            "cloudscale_consumer_is_leader",
            "1 when this process holds the consumer lease.",
        )
        self.lag_events = gauge(
            "cloudscale_consumer_lag_events",
            "Events in the log not yet applied to the projection (leader only).",
        )
        self.last_drain_timestamp = gauge(
            "cloudscale_consumer_last_drain_timestamp_seconds",
            "Unix time of the last successful drain pass (leader only).",
        )
        self.applied = counter(
            "cloudscale_consumer_events_applied_total",
            "Events applied to the projection.",
        )
        self.dead_lettered = counter(
            "cloudscale_consumer_events_dead_lettered_total",
            "Events parked in the dead-letter queue.",
        )
        self.halts = counter(
            "cloudscale_consumer_halts_total",
            "Drain passes halted by an open circuit.",
        )


def build_storage(
    storage: str,
) -> tuple[_ClosableFeed, _ClosableProjection, tuple[type[BaseException], ...], Lease]:
    """Return ``(feed, projection, retryable_errors, lease)`` for a storage tier."""
    consumer = os.environ.get("CLOUDSCALE_CONSUMER_NAME", "balances")
    if storage == "postgres":
        import psycopg

        from cloudscale.adapters.postgres.consumer_lease import PostgresConsumerLease
        from cloudscale.adapters.postgres.event_store import PostgresEventStore
        from cloudscale.adapters.postgres.projection_store import (
            PostgresProjectionStore,
        )

        dsn = os.environ["CLOUDSCALE_PG_DSN"]
        return (
            PostgresEventStore(dsn),
            PostgresProjectionStore(dsn, consumer=consumer),
            (psycopg.OperationalError,),
            PostgresConsumerLease(dsn, consumer=consumer),
        )
    if storage == "sqlite":
        import sqlite3

        from cloudscale.adapters.postgres.consumer_lease import NoLease
        from cloudscale.adapters.sqlite_compat.dead_letter_store import (
            DeadLetteringProjectionStore,
        )
        from cqrs import SqliteEventStore

        return (
            SqliteEventStore(os.environ["CLOUDSCALE_LOG_DB"]),
            DeadLetteringProjectionStore(
                path=os.environ["CLOUDSCALE_PROJECTION_DB"], consumer=consumer
            ),
            (sqlite3.OperationalError,),
            NoLease(),
        )
    raise ValueError(f"unsupported CLOUDSCALE_STORAGE: {storage!r}")


def run_forever(
    feed: _ClosableFeed,
    projection: _ClosableProjection,
    lease: Lease,
    *,
    retryable_errors: tuple[type[BaseException], ...],
    poll_interval: float,
    should_continue: Callable[[], bool],
    metrics: ConsumerMetrics | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Leader drains; standbys wait for the lease. Returns when told to stop."""
    consumer = ResilientConsumer(
        feed, projection, batch=500, retryable_errors=retryable_errors
    )
    was_leader = False
    while should_continue():
        if not lease.try_acquire():
            if was_leader:
                LOGGER.warning("consumer.lease.lost")
                was_leader = False
            if metrics:
                metrics.is_leader.set(0)
            sleep(poll_interval)
            continue
        if not was_leader:
            LOGGER.info("consumer.lease.acquired")
            was_leader = True
        if metrics:
            metrics.is_leader.set(1)

        report = consumer.run()
        if metrics:
            metrics.applied.inc(report.applied)
            metrics.dead_lettered.inc(report.dead_lettered)
            if report.halted:
                metrics.halts.inc()
            head = getattr(feed, "head_id", None)
            if head is not None:
                metrics.lag_events.set(max(0, head() - projection.last_id()))
            metrics.last_drain_timestamp.set(time.time())
        if report.applied == 0 or report.halted:
            sleep(poll_interval)


def main() -> int:
    storage = os.environ.get("CLOUDSCALE_STORAGE", "sqlite")
    poll_interval = float(
        os.environ.get("CLOUDSCALE_CONSUMER_POLL_SECONDS", _POLL_INTERVAL_SECONDS)
    )
    feed, projection, retryable, lease = build_storage(storage)

    metrics: ConsumerMetrics | None = None
    metrics_port = os.environ.get("CLOUDSCALE_CONSUMER_METRICS_PORT")
    if metrics_port:
        metrics = ConsumerMetrics(
            os.environ.get("CLOUDSCALE_CONSUMER_NAME", "balances")
        )
        start_http_server(int(metrics_port), registry=metrics.registry)

    running = True

    def _stop(_signum: int, _frame: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    try:
        run_forever(
            feed,
            projection,
            lease,
            retryable_errors=retryable,
            poll_interval=poll_interval,
            should_continue=lambda: running,
            metrics=metrics,
        )
    finally:
        lease.close()
        projection.close()
        feed.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
