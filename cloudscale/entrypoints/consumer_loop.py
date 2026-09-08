"""Continuously drain the durable log into the projection, as a real process.

    CLOUDSCALE_LOG_DB=... CLOUDSCALE_PROJECTION_DB=... \\
        python -m cloudscale.entrypoints.consumer_loop

Uses the Phase 2 resilient consumer per drain pass, so poison events are
dead-lettered and an open circuit pauses (without losing the offset) until
the next pass. Exits cleanly on SIGTERM/SIGINT.
"""

from __future__ import annotations

import os
import signal
import time

from cloudscale.adapters.sqlite_compat.dead_letter_store import (
    DeadLetteringProjectionStore,
)
from cloudscale.processes.resilient_consumer import ResilientConsumer
from cqrs import SqliteEventStore

_POLL_INTERVAL_SECONDS = 0.02


def main() -> int:
    log_db = os.environ["CLOUDSCALE_LOG_DB"]
    projection_db = os.environ["CLOUDSCALE_PROJECTION_DB"]
    poll_interval = float(
        os.environ.get("CLOUDSCALE_CONSUMER_POLL_SECONDS", _POLL_INTERVAL_SECONDS)
    )

    running = True

    def _stop(_signum: int, _frame: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    feed = SqliteEventStore(log_db)
    projection = DeadLetteringProjectionStore(path=projection_db)
    consumer = ResilientConsumer(feed, projection, batch=500)
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
