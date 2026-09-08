"""cqrs — CQRS command/query split with a durable event log.

Write path (commands) appends events to an append-only event log; read path
(projections) folds those events into read models. Phase 0 shipped an in-memory
stdlib store; Phase 1 adds a *durable* SQLite-backed log (``SqliteEventStore``)
and an *idempotent* projection consumer (``IdempotentProjectionStore``) that
turns at-least-once delivery into exactly-once read-model effect. Kafka and
PostgreSQL remain deferred per the ROADMAP; SQLite is the stdlib-only
realization of the same durable-log + idempotent-consumer tier.
"""

from typing import TYPE_CHECKING

from .commands import CommandError, CommandHandler

if TYPE_CHECKING:
    from cloudscale.adapters.sqlite_compat.event_store import (
        ConcurrencyError,
        EventStore,
        SqliteEventStore,
    )
    from cloudscale.adapters.sqlite_compat.projection_store import (
        BalanceProjection,
        IdempotentProjectionStore,
        run_consumer,
    )

_COMPATIBILITY_EXPORTS = frozenset(
    {
        "EventStore",
        "SqliteEventStore",
        "ConcurrencyError",
        "BalanceProjection",
        "IdempotentProjectionStore",
        "run_consumer",
    }
)


def __getattr__(name: str) -> object:
    """Load adapter-backed exports lazily so adapter modules import directly."""

    if name not in _COMPATIBILITY_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    from cloudscale.adapters.sqlite_compat.event_store import (
        ConcurrencyError,
        EventStore,
        SqliteEventStore,
    )
    from cloudscale.adapters.sqlite_compat.projection_store import (
        BalanceProjection,
        IdempotentProjectionStore,
        run_consumer,
    )

    exports = {
        "EventStore": EventStore,
        "SqliteEventStore": SqliteEventStore,
        "ConcurrencyError": ConcurrencyError,
        "BalanceProjection": BalanceProjection,
        "IdempotentProjectionStore": IdempotentProjectionStore,
        "run_consumer": run_consumer,
    }
    globals().update(exports)
    return exports[name]


def __dir__() -> list[str]:
    return sorted(set(globals()) | _COMPATIBILITY_EXPORTS)


__all__ = [
    "EventStore",
    "SqliteEventStore",
    "ConcurrencyError",
    "CommandHandler",
    "CommandError",
    "BalanceProjection",
    "IdempotentProjectionStore",
    "run_consumer",
]

__version__ = "0.1.0"
