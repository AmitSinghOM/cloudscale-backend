"""Immutable Account events and their versioned persistence envelope."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Literal, cast
from uuid import UUID

from .upcasting import CURRENT_SCHEMA_VERSION
from .commands import (
    MAX_SIGNED_BIGINT,
    _validate_account_id,
    _validate_amount,
)


@dataclass(frozen=True, slots=True)
class Deposited:
    """Positive minor units were added to an account."""

    account_id: str
    amount: int

    def __post_init__(self) -> None:
        _validate_account_id(self.account_id)
        _validate_amount(self.amount)


@dataclass(frozen=True, slots=True)
class Withdrawn:
    """Positive minor units were removed from an account."""

    account_id: str
    amount: int

    def __post_init__(self) -> None:
        _validate_account_id(self.account_id)
        _validate_amount(self.amount)


def _validate_transfer_leg(event: TransferDebited | TransferCredited) -> None:
    _validate_account_id(event.account_id)
    _validate_amount(event.amount)
    _validate_uuid(event.transfer_id, "transfer_id")
    _validate_account_id(event.counterparty)
    if event.counterparty == event.account_id:
        raise ValueError("a transfer leg's counterparty must be a different account")


@dataclass(frozen=True, slots=True)
class TransferDebited:
    """Positive minor units left an account as one leg of a transfer (ADR-0011).

    ``transfer_id`` pairs this leg with the ``TransferCredited`` on the
    ``counterparty`` stream; both are appended in one transaction.
    """

    account_id: str
    amount: int
    transfer_id: UUID
    counterparty: str

    def __post_init__(self) -> None:
        _validate_transfer_leg(self)


@dataclass(frozen=True, slots=True)
class TransferCredited:
    """Positive minor units arrived in an account as one leg of a transfer."""

    account_id: str
    amount: int
    transfer_id: UUID
    counterparty: str

    def __post_init__(self) -> None:
        _validate_transfer_leg(self)


def _validate_expires_at(value: object) -> None:
    """``expires_at`` is stored as text so the fold never parses a clock value."""
    if not isinstance(value, str) or not value:
        raise ValueError("expires_at must be a UTC ISO-8601 string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("expires_at must be UTC")


HoldReleaseReason = Literal["voided", "expired", "partial"]
_HOLD_RELEASE_REASONS: frozenset[str] = frozenset({"voided", "expired", "partial"})


@dataclass(frozen=True, slots=True)
class HoldPlaced:
    """Funds reserved on an account for a later posting to ``counterparty`` (ADR-0014).

    ``hold_id`` is the command id of the ``Hold`` and becomes the
    ``transfer_id`` of the eventual posting. ``held`` rises; ``balance`` is
    untouched; ``available = balance - held`` falls.
    """

    account_id: str
    amount: int
    hold_id: UUID
    counterparty: str
    expires_at: str

    def __post_init__(self) -> None:
        _validate_account_id(self.account_id)
        _validate_amount(self.amount)
        _validate_uuid(self.hold_id, "hold_id")
        _validate_account_id(self.counterparty)
        if self.counterparty == self.account_id:
            raise ValueError("a hold's counterparty must be a different account")
        _validate_expires_at(self.expires_at)


@dataclass(frozen=True, slots=True)
class HoldReleased:
    """Reserved funds returned to ``available`` without moving (ADR-0014).

    ``reason`` is ``voided`` (the payer cancelled), ``expired`` (the sweeper
    ran past ``expires_at``) or ``partial`` (a post captured less than held).
    """

    account_id: str
    amount: int
    hold_id: UUID
    reason: HoldReleaseReason

    def __post_init__(self) -> None:
        _validate_account_id(self.account_id)
        _validate_amount(self.amount)
        _validate_uuid(self.hold_id, "hold_id")
        if self.reason not in _HOLD_RELEASE_REASONS:
            raise ValueError("reason must be voided, expired or partial")


@dataclass(frozen=True, slots=True)
class HoldPosted:
    """Reserved funds left the account as the debit leg of the hold's posting (ADR-0014).

    Paired with a ``TransferCredited`` on ``counterparty`` whose
    ``transfer_id`` is this ``hold_id``. Both ``balance`` and ``held`` fall.
    """

    account_id: str
    amount: int
    hold_id: UUID
    counterparty: str

    def __post_init__(self) -> None:
        _validate_account_id(self.account_id)
        _validate_amount(self.amount)
        _validate_uuid(self.hold_id, "hold_id")
        _validate_account_id(self.counterparty)
        if self.counterparty == self.account_id:
            raise ValueError("a hold's counterparty must be a different account")


AccountEvent = (
    Deposited
    | Withdrawn
    | TransferDebited
    | TransferCredited
    | HoldPlaced
    | HoldReleased
    | HoldPosted
)
EventType = Literal[
    "Deposited",
    "Withdrawn",
    "TransferDebited",
    "TransferCredited",
    "HoldPlaced",
    "HoldReleased",
    "HoldPosted",
]
_EVENT_TYPES: dict[type, EventType] = {
    Deposited: "Deposited",
    Withdrawn: "Withdrawn",
    TransferDebited: "TransferDebited",
    TransferCredited: "TransferCredited",
    HoldPlaced: "HoldPlaced",
    HoldReleased: "HoldReleased",
    HoldPosted: "HoldPosted",
}
_TRANSFER_PAYLOAD_KEYS = frozenset(
    {"account_id", "amount", "transfer_id", "counterparty"}
)
_CASH_PAYLOAD_KEYS = frozenset({"account_id", "amount"})
_HOLD_PLACED_PAYLOAD_KEYS = frozenset(
    {"account_id", "amount", "hold_id", "counterparty", "expires_at"}
)
_HOLD_RELEASED_PAYLOAD_KEYS = frozenset({"account_id", "amount", "hold_id", "reason"})
_HOLD_POSTED_PAYLOAD_KEYS = frozenset(
    {"account_id", "amount", "hold_id", "counterparty"}
)

#: Direction each event type moves the balance of ``account_id``. Every
#: balance projection (typed fold, PostgreSQL and SQLite consumers, legacy
#: BalanceProjection) reads this table so a new type cannot be half-wired.
BALANCE_SIGN: Mapping[str, int] = MappingProxyType(
    {
        "Deposited": 1,
        "Withdrawn": -1,
        "TransferCredited": 1,
        "TransferDebited": -1,
        "HoldPlaced": 0,
        "HoldReleased": 0,
        "HoldPosted": -1,
    }
)

#: Direction each event type moves the ``held`` amount of ``account_id``
#: (ADR-0014). One table per projected quantity; ``available`` is derived.
HELD_SIGN: Mapping[str, int] = MappingProxyType(
    {
        "Deposited": 0,
        "Withdrawn": 0,
        "TransferCredited": 0,
        "TransferDebited": 0,
        "HoldPlaced": 1,
        "HoldReleased": -1,
        "HoldPosted": -1,
    }
)


def _validate_positive_version(value: object, field_name: str) -> None:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 1
        or value > MAX_SIGNED_BIGINT
    ):
        raise ValueError(
            f"{field_name} must be a positive, non-Boolean signed-BIGINT integer"
        )


def _validate_uuid(value: object, field_name: str) -> None:
    if not isinstance(value, UUID):
        raise ValueError(f"{field_name} must be a UUID")


def _validate_utc_timestamp(value: object, field_name: str) -> None:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must use UTC")


def _parse_utc_timestamp(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an RFC 3339 string")
    normalized = f"{value[:-1]}+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise ValueError(f"{field_name} must be a valid RFC 3339 timestamp") from error
    _validate_utc_timestamp(parsed, field_name)
    return parsed


def _format_utc_timestamp(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _freeze_json_value(value: object, path: str = "payload") -> object:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} cannot contain non-finite numbers")
        return value
    if isinstance(value, Mapping):
        frozen: dict[str, object] = {}
        for key, nested in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path} keys must be strings")
            frozen[key] = _freeze_json_value(nested, f"{path}.{key}")
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(
            _freeze_json_value(item, f"{path}[{index}]")
            for index, item in enumerate(value)
        )
    raise ValueError(f"{path} contains a non-JSON value: {type(value).__name__}")


def _thaw_json_value(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_json_value(nested) for key, nested in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json_value(item) for item in value]
    return value


def _decode_uuid(value: object, field_name: str) -> UUID:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a UUID string")
    try:
        return UUID(value)
    except ValueError as error:
        raise ValueError(f"{field_name} must be a valid UUID string") from error


def _decode_json_object(serialized: str) -> Mapping[str, object]:
    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON number is not allowed: {value}")

    try:
        decoded = json.loads(serialized, parse_constant=reject_constant)
    except json.JSONDecodeError as error:
        raise ValueError("serialized value must be valid JSON") from error
    if not isinstance(decoded, Mapping):
        raise ValueError("serialized value must contain a JSON object")
    return cast(Mapping[str, object], decoded)


def _canonical_json(value: Mapping[str, object]) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _event_payload(event: AccountEvent) -> dict[str, object]:
    payload: dict[str, object] = {
        "account_id": event.account_id,
        "amount": event.amount,
    }
    if isinstance(event, (TransferDebited, TransferCredited)):
        payload["transfer_id"] = str(event.transfer_id)
        payload["counterparty"] = event.counterparty
    elif isinstance(event, HoldPlaced):
        payload["hold_id"] = str(event.hold_id)
        payload["counterparty"] = event.counterparty
        payload["expires_at"] = event.expires_at
    elif isinstance(event, HoldReleased):
        payload["hold_id"] = str(event.hold_id)
        payload["reason"] = event.reason
    elif isinstance(event, HoldPosted):
        payload["hold_id"] = str(event.hold_id)
        payload["counterparty"] = event.counterparty
    return payload


def _event_type(event: AccountEvent) -> EventType:
    return _EVENT_TYPES[type(event)]


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    """Versioned immutable identity and trace metadata around one Account event."""

    event_id: UUID
    stream_id: str
    stream_version: int
    event_type: EventType
    occurred_at: datetime
    correlation_id: UUID
    causation_id: UUID
    command_id: UUID
    schema_version: int
    payload: Mapping[str, object]

    def __post_init__(self) -> None:
        _validate_uuid(self.event_id, "event_id")
        _validate_account_id(self.stream_id)
        _validate_positive_version(self.stream_version, "stream_version")
        if self.event_type not in _EVENT_TYPES.values():
            raise ValueError(
                "event_type must be Deposited, Withdrawn, TransferDebited or "
                "TransferCredited"
            )
        _validate_utc_timestamp(self.occurred_at, "occurred_at")
        _validate_uuid(self.correlation_id, "correlation_id")
        _validate_uuid(self.causation_id, "causation_id")
        _validate_uuid(self.command_id, "command_id")
        _validate_positive_version(self.schema_version, "schema_version")
        if not isinstance(self.payload, Mapping):
            raise ValueError("payload must be a mapping")

        frozen_payload = cast(Mapping[str, object], _freeze_json_value(self.payload))
        object.__setattr__(self, "payload", frozen_payload)

        event = self.to_domain_event()
        if event.account_id != self.stream_id:
            raise ValueError("payload account_id must match stream_id exactly")

    @classmethod
    def from_domain_event(
        cls,
        event: AccountEvent,
        *,
        event_id: UUID,
        stream_version: int,
        occurred_at: datetime,
        correlation_id: UUID,
        causation_id: UUID,
        command_id: UUID,
        schema_version: int | None = None,
    ) -> EventEnvelope:
        """Protect one typed Account event with caller-assigned identities.

        ``schema_version`` defaults to the shape this build writes for the
        event's type (``CURRENT_SCHEMA_VERSION``, ADR-0009). It must never be
        a literal: readers upcast *from* the stored stamp, so a writer that
        stamps v1 on a v2-shaped payload corrupts every later fold.
        """

        if type(event) not in _EVENT_TYPES:
            raise ValueError(
                "event must be Deposited, Withdrawn, TransferDebited or TransferCredited"
            )
        event_type = _event_type(event)
        if schema_version is None:
            schema_version = CURRENT_SCHEMA_VERSION[event_type]
        return cls(
            event_id=event_id,
            stream_id=event.account_id,
            stream_version=stream_version,
            event_type=event_type,
            occurred_at=occurred_at,
            correlation_id=correlation_id,
            causation_id=causation_id,
            command_id=command_id,
            schema_version=schema_version,
            payload=_event_payload(event),
        )

    def to_domain_event(self) -> AccountEvent:
        """Reconstruct the typed event without changing envelope identity."""

        account_id = cast(str, self.payload.get("account_id"))
        amount = cast(int, self.payload.get("amount"))
        if self.event_type in ("TransferDebited", "TransferCredited"):
            if set(self.payload) != _TRANSFER_PAYLOAD_KEYS:
                raise ValueError(
                    "v1 transfer payload must contain exactly account_id, amount, "
                    "transfer_id and counterparty"
                )
            transfer_id = _decode_uuid(self.payload["transfer_id"], "transfer_id")
            counterparty = cast(str, self.payload["counterparty"])
            leg = (
                TransferDebited
                if self.event_type == "TransferDebited"
                else TransferCredited
            )
            return leg(
                account_id=account_id,
                amount=amount,
                transfer_id=transfer_id,
                counterparty=counterparty,
            )
        if self.event_type in ("HoldPlaced", "HoldReleased", "HoldPosted"):
            return self._hold_event(account_id, amount)
        if set(self.payload) != _CASH_PAYLOAD_KEYS:
            raise ValueError("v1 payload must contain exactly account_id and amount")
        if self.event_type == "Deposited":
            return Deposited(account_id=account_id, amount=amount)
        return Withdrawn(account_id=account_id, amount=amount)

    def _hold_event(self, account_id: str, amount: int) -> AccountEvent:
        expected = {
            "HoldPlaced": _HOLD_PLACED_PAYLOAD_KEYS,
            "HoldReleased": _HOLD_RELEASED_PAYLOAD_KEYS,
            "HoldPosted": _HOLD_POSTED_PAYLOAD_KEYS,
        }[self.event_type]
        if set(self.payload) != expected:
            raise ValueError(
                f"v1 {self.event_type} payload must contain exactly {sorted(expected)}"
            )
        hold_id = _decode_uuid(self.payload["hold_id"], "hold_id")
        if self.event_type == "HoldPlaced":
            return HoldPlaced(
                account_id=account_id,
                amount=amount,
                hold_id=hold_id,
                counterparty=cast(str, self.payload["counterparty"]),
                expires_at=cast(str, self.payload["expires_at"]),
            )
        if self.event_type == "HoldReleased":
            return HoldReleased(
                account_id=account_id,
                amount=amount,
                hold_id=hold_id,
                reason=cast(HoldReleaseReason, self.payload["reason"]),
            )
        return HoldPosted(
            account_id=account_id,
            amount=amount,
            hold_id=hold_id,
            counterparty=cast(str, self.payload["counterparty"]),
        )

    def to_dict(self) -> dict[str, object]:
        """Return the complete v1 wire/storage representation."""

        return {
            "event_id": str(self.event_id),
            "stream_id": self.stream_id,
            "stream_version": self.stream_version,
            "event_type": self.event_type,
            "occurred_at": _format_utc_timestamp(self.occurred_at),
            "correlation_id": str(self.correlation_id),
            "causation_id": str(self.causation_id),
            "command_id": str(self.command_id),
            "schema_version": self.schema_version,
            "payload": _thaw_json_value(self.payload),
        }

    def to_json(self) -> str:
        """Serialize canonically for stable persistence and transport bytes."""

        return _canonical_json(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> EventEnvelope:
        """Validate and reconstruct a complete v1 representation."""

        required = {
            "event_id",
            "stream_id",
            "stream_version",
            "event_type",
            "occurred_at",
            "correlation_id",
            "causation_id",
            "command_id",
            "schema_version",
            "payload",
        }
        if set(value) != required:
            missing = sorted(required - set(value))
            extra = sorted(set(value) - required)
            raise ValueError(
                f"invalid event envelope fields; missing={missing}, extra={extra}"
            )
        payload = value["payload"]
        if not isinstance(payload, Mapping):
            raise ValueError("payload must be a mapping")
        return cls(
            event_id=_decode_uuid(value["event_id"], "event_id"),
            stream_id=cast(str, value["stream_id"]),
            stream_version=cast(int, value["stream_version"]),
            event_type=cast(EventType, value["event_type"]),
            occurred_at=_parse_utc_timestamp(value["occurred_at"], "occurred_at"),
            correlation_id=_decode_uuid(value["correlation_id"], "correlation_id"),
            causation_id=_decode_uuid(value["causation_id"], "causation_id"),
            command_id=_decode_uuid(value["command_id"], "command_id"),
            schema_version=cast(int, value["schema_version"]),
            payload=cast(Mapping[str, object], payload),
        )

    @classmethod
    def from_json(cls, serialized: str) -> EventEnvelope:
        """Deserialize canonical or equivalent JSON through full validation."""

        return cls.from_dict(_decode_json_object(serialized))


__all__ = [
    "BALANCE_SIGN",
    "HELD_SIGN",
    "AccountEvent",
    "Deposited",
    "EventEnvelope",
    "EventType",
    "HoldPlaced",
    "HoldPosted",
    "HoldReleaseReason",
    "HoldReleased",
    "TransferCredited",
    "TransferDebited",
    "Withdrawn",
]
