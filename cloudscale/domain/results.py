"""Immutable persisted outcomes of Account command processing."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import cast
from uuid import UUID

from .commands import (
    MAX_SIGNED_BIGINT,
    _validate_account_id,
    _validate_expected_version,
)
from .events import (
    _canonical_json,
    _decode_json_object,
    _decode_uuid,
    _format_utc_timestamp,
    _parse_utc_timestamp,
    _validate_utc_timestamp,
    _validate_uuid,
)


class CommandOutcome(StrEnum):
    """Stable persisted outcomes returned for original and replayed commands."""

    ACCEPTED = "accepted"
    VERSION_CONFLICT = "version_conflict"
    COMMAND_ID_CONFLICT = "command_id_conflict"
    INSUFFICIENT_FUNDS = "insufficient_funds"
    DOMAIN_REJECTED = "domain_rejected"


def _validate_current_version(value: object) -> None:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
        or value > MAX_SIGNED_BIGINT
    ):
        raise ValueError(
            "current_version must be a non-negative, non-Boolean signed-BIGINT integer"
        )


def _validate_http_status(value: object) -> None:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 100
        or value > 599
    ):
        raise ValueError("http_status must be a non-Boolean integer from 100 to 599")


@dataclass(frozen=True, slots=True)
class BalanceView:
    """Immutable balance projection returned by the query application service."""

    account_id: str
    balance: int
    version: int

    def __post_init__(self) -> None:
        _validate_account_id(self.account_id)
        if (
            not isinstance(self.balance, int)
            or isinstance(self.balance, bool)
            or self.balance < 0
            or self.balance > MAX_SIGNED_BIGINT
        ):
            raise ValueError(
                "balance must be a non-negative, non-Boolean signed-BIGINT integer"
            )
        _validate_current_version(self.version)


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Persisted command decision, including transport-neutral response metadata."""

    command_id: UUID
    request_hash: bytes
    outcome: CommandOutcome
    account_id: str
    expected_version: int
    current_version: int
    committed_version: int | None
    event_id: UUID | None
    correlation_id: UUID
    error_code: str | None
    http_status: int
    created_at: datetime

    def __post_init__(self) -> None:
        _validate_uuid(self.command_id, "command_id")
        if not isinstance(self.request_hash, bytes) or len(self.request_hash) != 32:
            raise ValueError("request_hash must be an immutable 32-byte SHA-256 digest")
        if not isinstance(self.outcome, CommandOutcome):
            try:
                outcome = CommandOutcome(self.outcome)
            except (TypeError, ValueError) as error:
                raise ValueError(
                    "outcome is not a supported command outcome"
                ) from error
            object.__setattr__(self, "outcome", outcome)

        _validate_account_id(self.account_id)
        _validate_expected_version(self.expected_version)
        _validate_current_version(self.current_version)
        _validate_uuid(self.correlation_id, "correlation_id")
        _validate_http_status(self.http_status)
        _validate_utc_timestamp(self.created_at, "created_at")

        if self.committed_version is not None:
            if (
                not isinstance(self.committed_version, int)
                or isinstance(self.committed_version, bool)
                or self.committed_version < 1
                or self.committed_version > MAX_SIGNED_BIGINT
            ):
                raise ValueError(
                    "committed_version must be a positive, non-Boolean "
                    "signed-BIGINT integer when present"
                )
        if self.event_id is not None:
            _validate_uuid(self.event_id, "event_id")
        if self.error_code is not None and (
            not isinstance(self.error_code, str) or self.error_code == ""
        ):
            raise ValueError("error_code must be None or a non-empty string")

        if self.outcome is CommandOutcome.ACCEPTED:
            if self.committed_version is None or self.event_id is None:
                raise ValueError(
                    "an accepted result requires committed_version and event_id"
                )
            if self.error_code is not None:
                raise ValueError("an accepted result cannot contain error_code")
            if not 200 <= self.http_status < 300:
                raise ValueError("an accepted result requires a successful HTTP status")
        else:
            if self.committed_version is not None or self.event_id is not None:
                raise ValueError(
                    "a rejected result cannot contain committed_version or event_id"
                )
            if self.error_code is None:
                raise ValueError("a rejected result requires a stable error_code")
            if not 400 <= self.http_status < 500:
                raise ValueError(
                    "a rejected result requires a client-error HTTP status"
                )

    @property
    def accepted(self) -> bool:
        """Whether the persisted command decision appended an event."""

        return self.outcome is CommandOutcome.ACCEPTED

    @property
    def rejected(self) -> bool:
        """Whether the persisted command decision appended no event."""

        return not self.accepted

    def to_dict(self) -> dict[str, object]:
        """Return the complete deterministic persistence representation."""

        return {
            "command_id": str(self.command_id),
            "request_hash": self.request_hash.hex(),
            "outcome": self.outcome.value,
            "account_id": self.account_id,
            "expected_version": self.expected_version,
            "current_version": self.current_version,
            "committed_version": self.committed_version,
            "event_id": str(self.event_id) if self.event_id is not None else None,
            "correlation_id": str(self.correlation_id),
            "error_code": self.error_code,
            "http_status": self.http_status,
            "created_at": _format_utc_timestamp(self.created_at),
        }

    def to_json(self) -> str:
        """Serialize canonically so persisted/replayed response bytes are stable."""

        return _canonical_json(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> CommandResult:
        """Validate and reconstruct a complete persisted result."""

        required = {
            "command_id",
            "request_hash",
            "outcome",
            "account_id",
            "expected_version",
            "current_version",
            "committed_version",
            "event_id",
            "correlation_id",
            "error_code",
            "http_status",
            "created_at",
        }
        if set(value) != required:
            missing = sorted(required - set(value))
            extra = sorted(set(value) - required)
            raise ValueError(
                f"invalid command result fields; missing={missing}, extra={extra}"
            )

        request_hash = value["request_hash"]
        if not isinstance(request_hash, str):
            raise ValueError("request_hash must be a hexadecimal string")
        try:
            digest = bytes.fromhex(request_hash)
        except ValueError as error:
            raise ValueError("request_hash must be a hexadecimal string") from error

        event_id_value = value["event_id"]
        if event_id_value is not None and not isinstance(event_id_value, str):
            raise ValueError("event_id must be null or a UUID string")
        error_code = value["error_code"]
        if error_code is not None and not isinstance(error_code, str):
            raise ValueError("error_code must be null or a string")

        return cls(
            command_id=_decode_uuid(value["command_id"], "command_id"),
            request_hash=digest,
            outcome=cast(CommandOutcome, value["outcome"]),
            account_id=cast(str, value["account_id"]),
            expected_version=cast(int, value["expected_version"]),
            current_version=cast(int, value["current_version"]),
            committed_version=cast(int | None, value["committed_version"]),
            event_id=(
                _decode_uuid(event_id_value, "event_id")
                if event_id_value is not None
                else None
            ),
            correlation_id=_decode_uuid(value["correlation_id"], "correlation_id"),
            error_code=error_code,
            http_status=cast(int, value["http_status"]),
            created_at=_parse_utc_timestamp(value["created_at"], "created_at"),
        )

    @classmethod
    def from_json(cls, serialized: str) -> CommandResult:
        """Deserialize canonical or equivalent JSON through full validation."""

        return cls.from_dict(_decode_json_object(serialized))


__all__ = ["BalanceView", "CommandOutcome", "CommandResult"]
