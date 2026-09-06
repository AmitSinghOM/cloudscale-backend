"""cqrs — Phase 0 command/query split.

Write path (commands) appends events to an append-only event log; read path
(projections) folds those events into read models. In-memory and stdlib-only;
later phases swap the store for Kafka and the read models for PostgreSQL.
"""

from .commands import CommandError, CommandHandler
from .eventstore import EventStore
from .projections import BalanceProjection

__all__ = [
    "EventStore",
    "CommandHandler",
    "CommandError",
    "BalanceProjection",
]

__version__ = "0.0.1"
