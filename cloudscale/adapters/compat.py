"""Conversions between the legacy dictionary API and typed Account events."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from cloudscale.domain.events import AccountEvent, Deposited, Withdrawn

SQLITE_COMPATIBILITY_METADATA: Mapping[str, object] = MappingProxyType(
    {
        "environment": "non-production",
        "storage_tier": "sqlite-compatibility",
        "production": False,
    }
)


def sqlite_compatibility_metadata() -> dict[str, object]:
    """Return mutable response metadata for the non-production SQLite tier."""

    return dict(SQLITE_COMPATIBILITY_METADATA)


def legacy_event_to_domain(event: Mapping[str, object]) -> AccountEvent:
    """Validate and convert one known legacy dictionary event to a domain value."""

    if not isinstance(event, Mapping):
        raise TypeError("event must be a mapping")

    event_type = event.get("type")
    account_id = event.get("account_id")
    amount = event.get("amount")
    if event_type == "Deposited":
        return Deposited(account_id=account_id, amount=amount)  # type: ignore[arg-type]
    if event_type == "Withdrawn":
        return Withdrawn(account_id=account_id, amount=amount)  # type: ignore[arg-type]
    raise ValueError(f"unsupported legacy event type: {event_type!r}")


def domain_event_to_legacy(
    event: AccountEvent,
    metadata: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Convert a typed event to a legacy dictionary while retaining metadata."""

    legacy = dict(metadata or {})
    if isinstance(event, Deposited):
        event_type = "Deposited"
    elif isinstance(event, Withdrawn):
        event_type = "Withdrawn"
    else:  # pragma: no cover - defensive guard for dynamically typed callers
        raise TypeError("event must be Deposited or Withdrawn")

    legacy.update(
        {
            "type": event_type,
            "account_id": event.account_id,
            "amount": event.amount,
        }
    )
    return legacy


def adapt_legacy_event(event: dict) -> dict:
    """Round-trip known events through domain values and copy unknown events.

    Unknown event dictionaries remain pass-through values so the legacy balance
    projections retain their established forward-compatible version behavior.
    """

    if not isinstance(event, dict):
        raise TypeError("event must be a dict")
    if event.get("type") not in ("Deposited", "Withdrawn"):
        return dict(event)
    return domain_event_to_legacy(legacy_event_to_domain(event), metadata=event)


__all__ = [
    "SQLITE_COMPATIBILITY_METADATA",
    "adapt_legacy_event",
    "domain_event_to_legacy",
    "legacy_event_to_domain",
    "sqlite_compatibility_metadata",
]
