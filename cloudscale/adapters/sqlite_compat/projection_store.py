"""Legacy projection interfaces backed by typed event conversions."""

from __future__ import annotations

from typing import Any, Dict, List

from cqrs.idempotent_consumer import (
    IdempotentProjectionStore as _LegacyIdempotentProjectionStore,
)
from cqrs.idempotent_consumer import run_consumer as _legacy_run_consumer
from cqrs.projections import BalanceProjection as _LegacyBalanceProjection

from cloudscale.adapters.compat import (
    adapt_legacy_event,
    sqlite_compatibility_metadata,
)


class BalanceProjection(_LegacyBalanceProjection):
    """Legacy dictionary projection with typed known-event validation."""

    def apply(self, state: Dict, event: dict) -> Dict:
        return super().apply(state, adapt_legacy_event(event))

    def rebuild(self, events: List[dict]) -> Dict:
        state = self.initial()
        for event in events:
            state = self.apply(state, event)
        return state


class IdempotentProjectionStore(_LegacyIdempotentProjectionStore):
    """Non-production SQLite projection preserving dedupe and checkpoints."""

    def apply(self, event: dict) -> bool:
        return super().apply(adapt_legacy_event(event))

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


def run_consumer(
    store: Any, projection: IdempotentProjectionStore, batch: int = 100
) -> int:
    """Run the legacy polling loop against compatibility adapter instances."""

    return _legacy_run_consumer(store, projection, batch)


__all__ = ["BalanceProjection", "IdempotentProjectionStore", "run_consumer"]
