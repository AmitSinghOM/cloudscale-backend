"""Conversions between the legacy dictionary API and typed Account events."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from uuid import UUID

from cloudscale.domain.events import (
    AccountEvent,
    Deposited,
    HoldPlaced,
    HoldPosted,
    HoldReleased,
    TransferCredited,
    TransferDebited,
    Withdrawn,
)

_KNOWN_TYPES = frozenset(
    {
        "Deposited",
        "Withdrawn",
        "TransferDebited",
        "TransferCredited",
        "HoldPlaced",
        "HoldReleased",
        "HoldPosted",
    }
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
    if event_type in ("HoldPlaced", "HoldReleased", "HoldPosted"):
        # A hold's id rides the ``transfer_id`` column: it becomes the
        # transfer id of the eventual posting (ADR-0014).
        hold_id = event.get("transfer_id")
        if isinstance(hold_id, str):
            hold_id = UUID(hold_id)
        if event_type == "HoldPlaced":
            return HoldPlaced(
                account_id=account_id,  # type: ignore[arg-type]
                amount=amount,  # type: ignore[arg-type]
                hold_id=hold_id,  # type: ignore[arg-type]
                counterparty=event.get("counterparty"),  # type: ignore[arg-type]
                expires_at=event.get("expires_at"),  # type: ignore[arg-type]
            )
        if event_type == "HoldReleased":
            return HoldReleased(
                account_id=account_id,  # type: ignore[arg-type]
                amount=amount,  # type: ignore[arg-type]
                hold_id=hold_id,  # type: ignore[arg-type]
                reason=event.get("release_reason"),  # type: ignore[arg-type]
            )
        return HoldPosted(
            account_id=account_id,  # type: ignore[arg-type]
            amount=amount,  # type: ignore[arg-type]
            hold_id=hold_id,  # type: ignore[arg-type]
            counterparty=event.get("counterparty"),  # type: ignore[arg-type]
        )
    raise ValueError(f"unsupported legacy event type: {event_type!r}")


def domain_event_to_legacy(
    event: AccountEvent,
    metadata: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Convert a typed event to a legacy dictionary while retaining metadata."""

    legacy = dict(metadata or {})
    event_type = type(event).__name__
    if event_type not in _KNOWN_TYPES:  # pragma: no cover - defensive guard
        raise TypeError("event must be an AccountEvent")

    legacy.update(
        {
            "type": event_type,
            "account_id": event.account_id,
            "amount": event.amount,
        }
    )
    transfer_id, counterparty, expires_at, reason = event_row_fields(event)
    for key, value in (
        ("transfer_id", transfer_id),
        ("counterparty", counterparty),
        ("expires_at", expires_at),
        ("release_reason", reason),
    ):
        if value is not None:
            legacy[key] = value
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

    transfer_id, counterparty, _, _ = event_row_fields(event)
    return transfer_id, counterparty


def event_row_fields(
    event: AccountEvent,
) -> tuple[str | None, str | None, str | None, str | None]:
    """Return ``(transfer_id, counterparty, expires_at, release_reason)`` for the row.

    Hold events store their ``hold_id`` in ``transfer_id`` (ADR-0014); every
    other field is NULL where the event type has no such attribute.
    """

    if isinstance(event, (TransferDebited, TransferCredited)):
        return str(event.transfer_id), event.counterparty, None, None
    if isinstance(event, HoldPlaced):
        return str(event.hold_id), event.counterparty, event.expires_at, None
    if isinstance(event, HoldReleased):
        return str(event.hold_id), None, None, event.reason
    if isinstance(event, HoldPosted):
        return str(event.hold_id), event.counterparty, None, None
    return None, None, None, None


__all__ = [
    "SQLITE_COMPATIBILITY_METADATA",
    "adapt_legacy_event",
    "domain_event_to_legacy",
    "event_row_fields",
    "legacy_event_to_domain",
    "sqlite_compatibility_metadata",
    "transfer_leg_fields",
]
