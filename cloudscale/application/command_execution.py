"""The storage-agnostic command decision, shared by every unit of work.

Extracted from the SQLite and Postgres ``CommandUnitOfWork`` adapters, whose
decision bodies had drifted into structurally identical copies under
different names. The decision itself never touches storage directly — it
speaks through :class:`CommandDecisionStorage`, and each adapter supplies its
four operations plus the surrounding transaction/serialization machinery.

Contract implemented here (see ``application.ports.CommandUnitOfWork``):

- equal-hash replay returns the stored result unchanged;
- a reused command id with a different hash returns ``command_id_conflict``
  WITHOUT persisting (the original record stays authoritative);
- expected-version mismatches, insufficient funds, and other deterministic
  domain rejections are persisted WITHOUT appending;
- an accepted command appends exactly one enveloped event and persists the
  accepted result.

The caller MUST invoke :func:`execute_command_decision` inside one atomic
transaction so the fold can never go stale between read and append.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Protocol
from uuid import UUID

from cloudscale.domain.account import AccountState, decide
from cloudscale.domain.errors import DomainError, InsufficientFundsError
from cloudscale.domain.events import AccountEvent, EventEnvelope
from cloudscale.domain.results import CommandOutcome, CommandResult

from .ports import NormalizedCommand


class CommandDecisionStorage(Protocol):
    """The four storage operations a command decision needs."""

    def stored_result(self, command_id: UUID) -> CommandResult | None: ...

    def fold_stream(self, account_id: str) -> AccountState: ...

    def append_event(self, envelope: EventEnvelope, event: AccountEvent) -> None: ...

    def persist_result(self, result: CommandResult) -> None: ...


def execute_command_decision(
    storage: CommandDecisionStorage,
    request: NormalizedCommand,
    *,
    clock: Callable[[], datetime],
    event_id_factory: Callable[[], UUID],
) -> CommandResult:
    """Run one command decision against ``storage``; caller owns atomicity."""
    stored = storage.stored_result(request.command_id)
    if stored is not None:
        if stored.request_hash == request.request_hash:
            return stored
        # Different business request under a reused command id. Reject, but
        # never overwrite the original persisted record.
        return _rejection(
            request,
            CommandOutcome.COMMAND_ID_CONFLICT,
            error_code="command_id_conflict",
            http_status=409,
            current_version=storage.fold_stream(request.command.account_id).version,
            created_at=clock(),
        )

    state = storage.fold_stream(request.command.account_id)

    if request.command.expected_version != state.version:
        return _persisted_rejection(
            storage,
            request,
            CommandOutcome.VERSION_CONFLICT,
            error_code="version_conflict",
            http_status=409,
            current_version=state.version,
            created_at=clock(),
        )

    try:
        event = decide(state, request.command)
    except InsufficientFundsError as error:
        return _persisted_rejection(
            storage,
            request,
            CommandOutcome.INSUFFICIENT_FUNDS,
            error_code=error.code,
            http_status=422,
            current_version=state.version,
            created_at=clock(),
        )
    except DomainError as error:
        return _persisted_rejection(
            storage,
            request,
            CommandOutcome.DOMAIN_REJECTED,
            error_code=error.code,
            http_status=400,
            current_version=state.version,
            created_at=clock(),
        )

    occurred_at = clock()
    envelope = EventEnvelope.from_domain_event(
        event,
        event_id=event_id_factory(),
        stream_version=state.version + 1,
        occurred_at=occurred_at,
        correlation_id=request.correlation_id,
        causation_id=request.command_id,
        command_id=request.command_id,
    )
    storage.append_event(envelope, event)
    result = CommandResult(
        command_id=request.command_id,
        request_hash=request.request_hash,
        outcome=CommandOutcome.ACCEPTED,
        account_id=request.command.account_id,
        expected_version=request.command.expected_version,
        current_version=state.version,
        committed_version=envelope.stream_version,
        event_id=envelope.event_id,
        correlation_id=request.correlation_id,
        error_code=None,
        http_status=201,
        created_at=occurred_at,
    )
    storage.persist_result(result)
    return result


def _persisted_rejection(
    storage: CommandDecisionStorage,
    request: NormalizedCommand,
    outcome: CommandOutcome,
    *,
    error_code: str,
    http_status: int,
    current_version: int,
    created_at: datetime,
) -> CommandResult:
    result = _rejection(
        request,
        outcome,
        error_code=error_code,
        http_status=http_status,
        current_version=current_version,
        created_at=created_at,
    )
    storage.persist_result(result)
    return result


def _rejection(
    request: NormalizedCommand,
    outcome: CommandOutcome,
    *,
    error_code: str,
    http_status: int,
    current_version: int,
    created_at: datetime,
) -> CommandResult:
    return CommandResult(
        command_id=request.command_id,
        request_hash=request.request_hash,
        outcome=outcome,
        account_id=request.command.account_id,
        expected_version=request.command.expected_version,
        current_version=current_version,
        committed_version=None,
        event_id=None,
        correlation_id=request.correlation_id,
        error_code=error_code,
        http_status=http_status,
        created_at=created_at,
    )


__all__ = ["CommandDecisionStorage", "execute_command_decision"]
