"""The storage-agnostic command decision, shared by every unit of work.

Extracted from the SQLite and Postgres ``CommandUnitOfWork`` adapters, whose
decision bodies had drifted into structurally identical copies under
different names. The decision itself never touches storage directly — it
speaks through :class:`CommandDecisionStorage`, and each adapter supplies its
four operations plus the surrounding transaction/serialization machinery.

Contract implemented here (see ``application.ports.CommandUnitOfWork``):

- equal-hash replay returns the stored result unchanged (status included:
  a replayed 201 is a 201, byte-for-byte);
- a reused command id with a different hash returns ``command_id_conflict``
  WITHOUT persisting (the original record stays authoritative);
- expected-version mismatches, insufficient funds, and other deterministic
  domain rejections are persisted WITHOUT appending;
- an accepted command appends exactly one enveloped event and persists the
  accepted result — except a ``Transfer`` (ADR-0011), which appends exactly
  two (debit on the source stream, credit on the target stream), and a
  ``Post`` (ADR-0013) appends one per leg; every leg is recorded in
  ``postings``.

The caller MUST invoke :func:`execute_command_decision` inside one atomic
transaction so the fold can never go stale between read and append.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Protocol
from uuid import UUID

from cloudscale.domain.account import AccountState, decide, decide_postings
from cloudscale.domain.commands import Post, Transfer
from cloudscale.domain.errors import DomainError, InsufficientFundsError
from cloudscale.domain.events import AccountEvent, EventEnvelope
from cloudscale.domain.results import CommandOutcome, CommandResult, Posting

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
        legs = _decide_legs(storage, request, state)
    except DomainError as error:
        outcome, http_status = _rejection_for(error)
        return _persisted_rejection(
            storage,
            request,
            outcome,
            error_code=error.code,
            http_status=http_status,
            current_version=state.version,
            created_at=clock(),
        )

    occurred_at = clock()
    postings: list[Posting] = []
    addressed: EventEnvelope | None = None
    for event, stream_version in legs:
        envelope = EventEnvelope.from_domain_event(
            event,
            event_id=event_id_factory(),
            stream_version=stream_version,
            occurred_at=occurred_at,
            correlation_id=request.correlation_id,
            causation_id=request.command_id,
            command_id=request.command_id,
        )
        storage.append_event(envelope, event)
        postings.append(Posting(event.account_id, envelope.event_id, stream_version))
        if event.account_id == request.command.account_id:
            addressed = envelope
    if addressed is None:  # pragma: no cover - every command writes its own stream
        raise RuntimeError("command produced no leg on its addressed stream")
    result = CommandResult(
        command_id=request.command_id,
        request_hash=request.request_hash,
        outcome=CommandOutcome.ACCEPTED,
        account_id=request.command.account_id,
        expected_version=request.command.expected_version,
        current_version=state.version,
        committed_version=addressed.stream_version,
        event_id=addressed.event_id,
        correlation_id=request.correlation_id,
        error_code=None,
        http_status=201,
        created_at=occurred_at,
        postings=tuple(postings),
    )
    storage.persist_result(result)
    return result


def _decide_legs(
    storage: CommandDecisionStorage, request: NormalizedCommand, state: AccountState
) -> list[tuple[AccountEvent, int]]:
    """Return the ``(event, stream_version)`` legs a valid command appends.

    Single-account commands yield one leg. A ``Transfer`` (ADR-0011) or a
    ``Post`` (ADR-0013) folds every other named stream too and yields one leg
    per posting, **sorted by account id**: that total order means concurrent
    posting sets over overlapping accounts wait on one ``(stream, seq)`` key
    instead of forming a lock cycle of any length. Non-anchor streams have no
    client-supplied version; their invariants are checked on this fresh fold
    and a concurrent writer is caught by ``UNIQUE (stream, seq)`` inside the
    same transaction (the unit of work retries on a fresh fold).
    """
    command = request.command
    if isinstance(command, (Transfer, Post)):
        states: dict[str, AccountState] = {command.account_id: state}
        for leg in command.legs():
            if leg.account_id not in states:
                states[leg.account_id] = storage.fold_stream(leg.account_id)
        events = decide_postings(states, command, transfer_id=request.command_id)
        legs = [(event, states[event.account_id].version + 1) for event in events]
        return sorted(legs, key=lambda leg: leg[0].account_id)
    return [(decide(state, command), state.version + 1)]


def _rejection_for(error: DomainError) -> tuple[CommandOutcome, int]:
    if isinstance(error, InsufficientFundsError):
        return CommandOutcome.INSUFFICIENT_FUNDS, 422
    return CommandOutcome.DOMAIN_REJECTED, 400


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
