"""Framework-independent Account command normalization and orchestration."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from typing import Final
from uuid import UUID, uuid4

from cloudscale.domain.commands import AccountCommand, Deposit, Withdraw
from cloudscale.domain.results import CommandResult

from .ports import CommandUnitOfWork, NormalizedCommand

COMMAND_SCHEMA_VERSION: Final = 1


def canonical_command_payload(
    command: AccountCommand, *, issuer: str, subject: str
) -> bytes:
    """Return the canonical UTF-8 JSON bytes used for command idempotency.

    Identity claims are exact and intentionally included. Command and correlation IDs
    are intentionally excluded so the command ID is not hashed into itself and trace
    metadata can change on a retry without changing the business request.
    """

    if not isinstance(command, (Deposit, Withdraw)):
        raise TypeError("command must be Deposit or Withdraw")
    if not isinstance(issuer, str) or issuer == "":
        raise ValueError("issuer must be a non-empty string")
    if not isinstance(subject, str) or subject == "":
        raise ValueError("subject must be a non-empty string")

    normalized = {
        "account_id": command.account_id,
        "amount": command.amount,
        "expected_version": command.expected_version,
        "issuer": issuer,
        "schema_version": COMMAND_SCHEMA_VERSION,
        "subject": subject,
        "type": type(command).__name__,
    }
    return json.dumps(
        normalized,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def command_request_hash(canonical_payload: bytes) -> bytes:
    """Return the SHA-256 digest of canonical command bytes."""

    if not isinstance(canonical_payload, bytes):
        raise TypeError("canonical_payload must be bytes")
    return hashlib.sha256(canonical_payload).digest()


def normalize_command(
    command: AccountCommand,
    *,
    command_id: UUID,
    correlation_id: UUID,
    issuer: str,
    subject: str,
) -> NormalizedCommand:
    """Build the immutable request passed to the atomic command unit of work."""

    canonical_payload = canonical_command_payload(
        command, issuer=issuer, subject=subject
    )
    return NormalizedCommand(
        command=command,
        command_id=command_id,
        correlation_id=correlation_id,
        issuer=issuer,
        subject=subject,
        canonical_payload=canonical_payload,
        request_hash=command_request_hash(canonical_payload),
    )


class CommandService:
    """Normalize one authorized command and delegate exactly one atomic execution."""

    def __init__(
        self,
        unit_of_work: CommandUnitOfWork,
        *,
        correlation_id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._correlation_id_factory = correlation_id_factory

    def execute(
        self,
        command: AccountCommand,
        *,
        command_id: UUID,
        issuer: str,
        subject: str,
        correlation_id: UUID | None = None,
    ) -> CommandResult:
        """Execute a command, generating correlation identity only when omitted."""

        effective_correlation_id = (
            self._correlation_id_factory() if correlation_id is None else correlation_id
        )
        request = normalize_command(
            command,
            command_id=command_id,
            correlation_id=effective_correlation_id,
            issuer=issuer,
            subject=subject,
        )
        return self._unit_of_work.execute(request)


__all__ = [
    "COMMAND_SCHEMA_VERSION",
    "CommandService",
    "canonical_command_payload",
    "command_request_hash",
    "normalize_command",
]
