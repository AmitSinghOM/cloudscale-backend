"""Continuously drain the durable log into the projection, as a real process.

    CLOUDSCALE_LOG_DB=... CLOUDSCALE_PROJECTION_DB=... \\
        python -m cloudscale.entrypoints.consumer_loop

Storage tier selection mirrors the HTTP server (``CLOUDSCALE_STORAGE``):
``sqlite`` (default, file paths above) or ``postgres``
(``CLOUDSCALE_PG_DSN``; retryable errors switch to psycopg's).

Uses the Phase 2 resilient consumer per drain pass, so poison events are
dead-lettered and an open circuit pauses (without losing the offset) until
the next pass. Exits cleanly on SIGTERM/SIGINT.
"""

from __future__ import annotations

import os
import signal
import time
from typing import Protocol

from cloudscale.processes.resilient_consumer import (
    DeadLetteringProjection,
    EventFeed,
    ResilientConsumer,
)

_POLL_INTERVAL_SECONDS = 0.02


class _ClosableFeed(EventFeed, Protocol):
    def close(self) -> None: ...


class _ClosableProjection(DeadLetteringProjection, Protocol):
    def close(self) -> None: ...


def build_storage(
    storage: str,
) -> tuple[_ClosableFeed, _ClosableProjection, tuple[type[BaseException], ...]]:
    """Return ``(feed, projection, retryable_errors)`` for a storage tier."""
    if storage == "postgres":
        import psycopg

        from cloudscale.adapters.postgres.event_store import PostgresEventStore
        from cloudscale.adapters.postgres.projection_store import (
            PostgresProjectionStore,
        )

        dsn = os.environ["CLOUDSCALE_PG_DSN"]
        return (
            PostgresEventStore(dsn),
            PostgresProjectionStore(dsn),
            (psycopg.OperationalError,),
        )
    if storage == "sqlite":
        import sqlite3

        from cloudscale.adapters.sqlite_compat.dead_letter_store import (
            DeadLetteringProjectionStore,
        )
        from cqrs import SqliteEventStore

        return (
            SqliteEventStore(os.environ["CLOUDSCALE_LOG_DB"]),
            DeadLetteringProjectionStore(path=os.environ["CLOUDSCALE_PROJECTION_DB"]),
            (sqlite3.OperationalError,),
        )
    raise ValueError(f"unsupported CLOUDSCALE_STORAGE: {storage!r}")


def main() -> int:
    storage = os.environ.get("CLOUDSCALE_STORAGE", "sqlite")
    poll_interval = float(
        os.environ.get("CLOUDSCALE_CONSUMER_POLL_SECONDS", _POLL_INTERVAL_SECONDS)
    )
    feed, projection, retryable = build_storage(storage)

    running = True

    def _stop(_signum: int, _frame: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    consumer = ResilientConsumer(
        feed, projection, batch=500, retryable_errors=retryable
    )
    try:
        while running:
            report = consumer.run()
            if report.applied == 0:
                time.sleep(poll_interval)
    finally:
        projection.close()
        feed.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
