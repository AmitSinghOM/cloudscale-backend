"""Typed structural ports implemented by infrastructure adapters."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import hashlib
from typing import Protocol, TypeVar
from uuid import UUID

from cloudscale.domain.commands import AccountCommand
from cloudscale.domain.events import EventEnvelope
from cloudscale.domain.results import BalanceView, CommandResult


@dataclass(frozen=True, slots=True)
class NormalizedCommand:
    """Complete, immutable input to an atomic command transaction.

    ``canonical_payload`` excludes command and correlation identifiers. This makes a
    retry trace-independent while keeping the idempotency key out of its own digest.
    Adapters persist both the bytes and digest so collisions can be diagnosed without
    reconstructing caller input.
    """

    command: AccountCommand
    command_id: UUID
    correlation_id: UUID
    issuer: str
    subject: str
    canonical_payload: bytes
    request_hash: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.command_id, UUID):
            raise ValueError("command_id must be a UUID")
        if not isinstance(self.correlation_id, UUID):
            raise ValueError("correlation_id must be a UUID")
        if not isinstance(self.issuer, str) or self.issuer == "":
            raise ValueError("issuer must be a non-empty string")
        if not isinstance(self.subject, str) or self.subject == "":
            raise ValueError("subject must be a non-empty string")
        if not isinstance(self.canonical_payload, bytes):
            raise ValueError("canonical_payload must be UTF-8 bytes")
        try:
            self.canonical_payload.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("canonical_payload must be UTF-8 bytes") from error
        if not isinstance(self.request_hash, bytes) or len(self.request_hash) != 32:
            raise ValueError("request_hash must be a 32-byte SHA-256 digest")
        if self.request_hash != hashlib.sha256(self.canonical_payload).digest():
            raise ValueError("request_hash must match canonical_payload")


class CommandUnitOfWork(Protocol):
    """Atomically decide, append, and persist the result for one command.

    Implementations own command-ID serialization. They return the original persisted
    result for an equal request hash, return ``command_id_conflict`` for a different
    hash, and persist deterministic domain/version rejections without appending.
    """

    def execute(self, request: NormalizedCommand) -> CommandResult: ...


class ProjectionReader(Protocol):
    """Read a projection, returning ``None`` when the account is absent."""

    def get_balance(self, account_id: str) -> BalanceView | None: ...


class EventReader(Protocol):
    """Read an ordered authoritative stream after an optional version."""

    def read_stream(
        self, stream_id: str, after_version: int = 0
    ) -> Sequence[EventEnvelope]: ...


_ProjectionDeliveryT = TypeVar("_ProjectionDeliveryT", contravariant=True)
_ProjectionDispositionT = TypeVar("_ProjectionDispositionT", covariant=True)


class ProjectionUnitOfWork(Protocol[_ProjectionDeliveryT, _ProjectionDispositionT]):
    """Apply one delivery and its projection progress atomically."""

    def apply(self, delivery: _ProjectionDeliveryT) -> _ProjectionDispositionT: ...


__all__ = [
    "CommandUnitOfWork",
    "EventReader",
    "NormalizedCommand",
    "ProjectionReader",
    "ProjectionUnitOfWork",
]
