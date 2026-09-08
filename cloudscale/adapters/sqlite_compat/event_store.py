"""Legacy event-store interfaces backed by typed event conversions."""

from __future__ import annotations

from typing import List, Optional

from cqrs.durable_eventstore import (
    ConcurrencyError,
    SqliteEventStore as _LegacySqliteEventStore,
)
from cqrs.eventstore import EventStore as _LegacyEventStore

from cloudscale.adapters.compat import (
    adapt_legacy_event,
    sqlite_compatibility_metadata,
)


class EventStore(_LegacyEventStore):
    """In-memory legacy dictionary store with typed known-event validation."""

    def append(self, stream: str, event: dict) -> int:
        return super().append(stream, adapt_legacy_event(event))

    def read(self, stream: str) -> List[dict]:
        return [adapt_legacy_event(event) for event in super().read(stream)]


class SqliteEventStore(_LegacySqliteEventStore):
    """Non-production SQLite compatibility store for legacy dictionaries."""

    def append(self, stream: str, event: dict) -> int:
        return super().append(stream, adapt_legacy_event(event))

    def read(self, stream: str) -> List[dict]:
        return [adapt_legacy_event(event) for event in super().read(stream)]

    def read_all(self, after_id: int = 0, limit: Optional[int] = None) -> List[dict]:
        return [
            adapt_legacy_event(event)
            for event in super().read_all(after_id=after_id, limit=limit)
        ]

    @property
    def metadata(self) -> dict[str, object]:
        """Metadata shared by startup and health reporting."""

        return sqlite_compatibility_metadata()

    def startup_metadata(self) -> dict[str, object]:
        """Return safe startup metadata identifying SQLite as non-production."""

        return self.metadata

    def health_metadata(self) -> dict[str, object]:
        """Return safe health metadata identifying SQLite as non-production."""

        return self.metadata


__all__ = ["ConcurrencyError", "EventStore", "SqliteEventStore"]
