"""Conversions between the legacy dictionary API and typed Account events."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from uuid import UUID

from cloudscale.domain.events import (
    AccountEvent,
    Deposited,
    TransferCredited,
    TransferDebited,
    Withdrawn,
)

_KNOWN_TYPES = frozenset(
    {"Deposited", "Withdrawn", "TransferDebited", "TransferCredited"}
)

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
    if event_type in ("TransferDebited", "TransferCredited"):
        transfer_id = event.get("transfer_id")
        if isinstance(transfer_id, str):
            transfer_id = UUID(transfer_id)
        leg = TransferDebited if event_type == "TransferDebited" else TransferCredited
        return leg(
            account_id=account_id,  # type: ignore[arg-type]
            amount=amount,  # type: ignore[arg-type]
            transfer_id=transfer_id,  # type: ignore[arg-type]
            counterparty=event.get("counterparty"),  # type: ignore[arg-type]
        )
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
    elif isinstance(event, TransferDebited):
        event_type = "TransferDebited"
    elif isinstance(event, TransferCredited):
        event_type = "TransferCredited"
    else:  # pragma: no cover - defensive guard for dynamically typed callers
        raise TypeError("event must be an AccountEvent")

    legacy.update(
        {
            "type": event_type,
            "account_id": event.account_id,
            "amount": event.amount,
        }
    )
    if isinstance(event, (TransferDebited, TransferCredited)):
        legacy["transfer_id"] = str(event.transfer_id)
        legacy["counterparty"] = event.counterparty
    return legacy


def adapt_legacy_event(event: dict) -> dict:
    """Round-trip known events through domain values and copy unknown events.

    Unknown event dictionaries remain pass-through values so the legacy balance
    projections retain their established forward-compatible version behavior.
    """

    if not isinstance(event, dict):
        raise TypeError("event must be a dict")
    if event.get("type") not in _KNOWN_TYPES:
        return dict(event)
    return domain_event_to_legacy(legacy_event_to_domain(event), metadata=event)


def transfer_leg_fields(event: AccountEvent) -> tuple[str | None, str | None]:
    """Return ``(transfer_id, counterparty)`` for the ``events`` row; NULLs otherwise."""

    if isinstance(event, (TransferDebited, TransferCredited)):
        return str(event.transfer_id), event.counterparty
    return None, None


__all__ = [
    "SQLITE_COMPATIBILITY_METADATA",
    "adapt_legacy_event",
    "domain_event_to_legacy",
    "legacy_event_to_domain",
    "sqlite_compatibility_metadata",
    "transfer_leg_fields",
]
