"""cqrs — CQRS command/query split with a durable event log.

Write path (commands) appends events to an append-only event log; read path
(projections) folds those events into read models. Phase 0 shipped an in-memory
stdlib store; Phase 1 adds a *durable* SQLite-backed log (``SqliteEventStore``)
and an *idempotent* projection consumer (``IdempotentProjectionStore``) that
turns at-least-once delivery into exactly-once read-model effect. Kafka and
PostgreSQL remain deferred per the ROADMAP; SQLite is the stdlib-only
realization of the same durable-log + idempotent-consumer tier.
"""

from .commands import CommandError, CommandHandler
from .durable_eventstore import ConcurrencyError, SqliteEventStore
from .eventstore import EventStore
from .idempotent_consumer import IdempotentProjectionStore, run_consumer
from .projections import BalanceProjection

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
