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
    held: int = 0

    @property
    def available(self) -> int:
        return self.balance - self.held

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
        if (
            not isinstance(self.held, int)
            or isinstance(self.held, bool)
            or self.held < 0
        ):
            raise ValueError("held must be a non-negative, non-Boolean integer")
        _validate_current_version(self.version)


@dataclass(frozen=True, slots=True)
class TransferLeg:
    """One leg of a committed posting set as the read model records it (ADR-0015)."""

    account_id: str
    amount: int
    direction: str  # "debit" | "credit"


@dataclass(frozen=True, slots=True)
class TransferView:
    """A committed posting set and whether a reversal names it (ADR-0015).

    ``kind`` is ``transfer`` (2 legs), ``posting`` (N legs), ``hold_posting``
    or ``reversal``. ``reverted_by`` is the reversal's ``transfer_id`` or
    ``None``; ``reverts`` is what this set undid, if it is itself a reversal.
    """

    transfer_id: UUID
    kind: str
    legs: tuple[TransferLeg, ...]
    reverted_by: UUID | None = None
    reverts: UUID | None = None


@dataclass(frozen=True, slots=True)
class Posting:
    """One stream written by an accepted command (ADR-0011).

    Single-account commands produce one posting; a transfer produces two
    (debit on the source, credit on the target), sharing the command id.
    """

    account_id: str
    event_id: UUID
    committed_version: int

    def __post_init__(self) -> None:
        _validate_account_id(self.account_id)
        _validate_uuid(self.event_id, "event_id")
        if (
            not isinstance(self.committed_version, int)
            or isinstance(self.committed_version, bool)
            or self.committed_version < 1
            or self.committed_version > MAX_SIGNED_BIGINT
        ):
            raise ValueError(
                "committed_version must be a positive, non-Boolean signed-BIGINT integer"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "account_id": self.account_id,
            "event_id": str(self.event_id),
            "committed_version": self.committed_version,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> Posting:
        if set(value) != {"account_id", "event_id", "committed_version"}:
            raise ValueError("invalid posting fields")
        return cls(
            account_id=cast(str, value["account_id"]),
            event_id=_decode_uuid(value["event_id"], "event_id"),
            committed_version=cast(int, value["committed_version"]),
        )


def _digest_from(raw: object) -> bytes:
    if not isinstance(raw, str):
        raise ValueError("request_hash must be a hexadecimal string")
    try:
        return bytes.fromhex(raw)
    except ValueError as error:
        raise ValueError("request_hash must be a hexadecimal string") from error


def _postings_from(raw: object) -> tuple[Posting, ...]:
    if not isinstance(raw, list):
        raise ValueError("postings must be a list")
    return tuple(Posting.from_dict(cast(Mapping[str, object], item)) for item in raw)


def _validated_postings(
    postings: tuple[Posting, ...], source: Posting
) -> tuple[Posting, ...]:
    """Return the postings of an accepted result, deriving the single one.

    ``source`` is the posting implied by the result's own account_id /
    event_id / committed_version; it must appear in ``postings`` unchanged,
    and no account may be posted twice.
    """
    if not postings:
        return (source,)
    if not all(isinstance(posting, Posting) for posting in postings):
        raise ValueError("postings must contain Posting values")
    if len({posting.account_id for posting in postings}) != len(postings):
        raise ValueError("postings must name each account at most once")
    if source not in postings:
        raise ValueError(
            "postings must include the addressed account with the result's "
            "event_id and committed_version"
        )
    return postings


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Persisted command decision, including transport-neutral response metadata.

    ``account_id``, ``committed_version`` and ``event_id`` describe the stream
    the command was addressed to (the source, for a transfer). ``postings``
    lists every stream an accepted command wrote, in append order; it is
    derived for single-posting results so every reader sees one shape.
    """

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
    postings: tuple[Posting, ...] = ()

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

        object.__setattr__(self, "postings", self._postings_for_outcome())

    def _postings_for_outcome(self) -> tuple[Posting, ...]:
        """Check the fields an outcome requires/forbids; return its postings."""
        if self.outcome is CommandOutcome.ACCEPTED:
            return self._accepted_postings()
        if self.committed_version is not None or self.event_id is not None:
            raise ValueError(
                "a rejected result cannot contain committed_version or event_id"
            )
        if self.error_code is None:
            raise ValueError("a rejected result requires a stable error_code")
        if not 400 <= self.http_status < 500:
            raise ValueError("a rejected result requires a client-error HTTP status")
        if tuple(self.postings):
            raise ValueError("a rejected result cannot contain postings")
        return ()

    def _accepted_postings(self) -> tuple[Posting, ...]:
        if self.committed_version is None or self.event_id is None:
            raise ValueError(
                "an accepted result requires committed_version and event_id"
            )
        if self.error_code is not None:
            raise ValueError("an accepted result cannot contain error_code")
        if not 200 <= self.http_status < 300:
            raise ValueError("an accepted result requires a successful HTTP status")
        source = Posting(self.account_id, self.event_id, self.committed_version)
        return _validated_postings(tuple(self.postings), source)

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
            "postings": [posting.to_dict() for posting in self.postings],
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
        # ``postings`` is optional: results persisted before ADR-0011 lack it and
        # the single posting is derived from the addressed account's fields.
        present = set(value) - {"postings"}
        if present != required:
            missing = sorted(required - present)
            extra = sorted(present - required)
            raise ValueError(
                f"invalid command result fields; missing={missing}, extra={extra}"
            )
        postings = _postings_from(value.get("postings", []))

        digest = _digest_from(value["request_hash"])
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
            postings=postings,
        )

    @classmethod
    def from_json(cls, serialized: str) -> CommandResult:
        """Deserialize canonical or equivalent JSON through full validation."""

        return cls.from_dict(_decode_json_object(serialized))


__all__ = ["BalanceView", "CommandOutcome", "CommandResult", "Posting"]
