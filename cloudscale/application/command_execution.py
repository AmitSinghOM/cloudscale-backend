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

from collections.abc import Callable, Iterable, Mapping
from datetime import datetime
from typing import Protocol
from uuid import UUID

from cloudscale.domain.account import (
    AccountState,
    OpenHold,
    decide,
    decide_hold,
    decide_post_hold,
    decide_postings,
    decide_release_hold,
    decide_revert,
)
from cloudscale.domain.commands import (
    ExpireHold,
    Hold,
    Post,
    PostHold,
    Revert,
    Transfer,
    VoidHold,
)
from cloudscale.domain.errors import (
    AlreadyRevertedError,
    DomainError,
    InsufficientFundsError,
)
from cloudscale.domain.events import AccountEvent, EventEnvelope
from cloudscale.domain.results import CommandOutcome, CommandResult, Posting

from .ports import NormalizedCommand


class CommandDecisionStorage(Protocol):
    """The storage operations a command decision needs."""

    def stored_result(self, command_id: UUID) -> CommandResult | None: ...

    def fold_stream(self, account_id: str) -> AccountState: ...

    def open_hold(self, account_id: str, hold_id: UUID) -> OpenHold | None:
        """The open hold derived from ``account_id``'s own events (ADR-0014)."""
        ...

    def legs_of(self, transfer_id: UUID) -> tuple[AccountEvent, ...]:
        """Every row of a posting set, across streams (ADR-0015)."""
        ...

    def reverted_by(self, transfer_id: UUID) -> UUID | None:
        """The reversal that names ``transfer_id``, if any (ADR-0015)."""
        ...

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

    occurred_at = clock()
    try:
        legs = _decide_legs(storage, request, state, now=occurred_at)
    except DomainError as error:
        outcome, http_status = _rejection_for(error)
        return _persisted_rejection(
            storage,
            request,
            outcome,
            error_code=error.code,
            http_status=http_status,
            current_version=state.version,
            created_at=occurred_at,
        )

    postings: dict[str, Posting] = {}
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
        # One posting per stream: when a command writes a stream twice (a
        # partial hold capture, ADR-0014) the posting names the last event.
        postings[event.account_id] = Posting(
            event.account_id, envelope.event_id, stream_version
        )
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
        postings=tuple(postings.values()),
    )
    storage.persist_result(result)
    return result


def _decide_legs(
    storage: CommandDecisionStorage,
    request: NormalizedCommand,
    state: AccountState,
    *,
    now: datetime,
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
        return _postings_legs(storage, command, state, command_id=request.command_id)
    if isinstance(command, Hold):
        placed = decide_hold(state, command, hold_id=request.command_id, now=now)
        return [(placed, state.version + 1)]
    if isinstance(command, PostHold):
        return _post_hold_legs(storage, state, command, now=now)
    if isinstance(command, (VoidHold, ExpireHold)):
        hold = storage.open_hold(command.account_id, command.hold_id)
        released = decide_release_hold(state, hold, command, now=now)
        return [(released, state.version + 1)]
    if isinstance(command, Revert):
        return _revert_legs(storage, command, state, command_id=request.command_id)
    return [(decide(state, command), state.version + 1)]


def _fold_others(
    storage: CommandDecisionStorage,
    anchor_id: str,
    anchor: AccountState,
    account_ids: Iterable[str],
) -> dict[str, AccountState]:
    """The anchor's already-folded state plus a fresh fold of every other stream."""
    states = {anchor_id: anchor}
    for account_id in account_ids:
        if account_id not in states:
            states[account_id] = storage.fold_stream(account_id)
    return states


def _postings_legs(
    storage: CommandDecisionStorage,
    command: Transfer | Post,
    state: AccountState,
    *,
    command_id: UUID,
) -> list[tuple[AccountEvent, int]]:
    states = _fold_others(
        storage, command.account_id, state, (leg.account_id for leg in command.legs())
    )
    return _number_legs(
        states, decide_postings(states, command, transfer_id=command_id)
    )


def _post_hold_legs(
    storage: CommandDecisionStorage,
    state: AccountState,
    command: PostHold,
    *,
    now: datetime,
) -> list[tuple[AccountEvent, int]]:
    hold = storage.open_hold(command.account_id, command.hold_id)
    # The target is known only from the hold itself; fold it once found.
    target_id = hold.counterparty if hold is not None else command.account_id
    states = _fold_others(
        storage, command.account_id, state, [target_id] if hold is not None else []
    )
    events = decide_post_hold(state, states[target_id], hold, command, now=now)
    return _number_legs(states, events)


def _revert_legs(
    storage: CommandDecisionStorage,
    command: Revert,
    state: AccountState,
    *,
    command_id: UUID,
) -> list[tuple[AccountEvent, int]]:
    original = storage.legs_of(command.transfer_id)
    states = _fold_others(
        storage, command.account_id, state, (leg.account_id for leg in original)
    )
    events = decide_revert(
        states,
        original,
        command,
        transfer_id=command_id,
        reverted_by=storage.reverted_by(command.transfer_id),
    )
    return _number_legs(states, events)


def _number_legs(
    states: Mapping[str, AccountState], events: tuple[AccountEvent, ...]
) -> list[tuple[AccountEvent, int]]:
    """Assign each event its stream version (several may hit one stream) and sort.

    ``sorted`` is stable, so two events on one stream keep their decision order.
    """
    next_version = {account: s.version for account, s in states.items()}
    legs: list[tuple[AccountEvent, int]] = []
    for event in events:
        next_version[event.account_id] += 1
        legs.append((event, next_version[event.account_id]))
    return sorted(legs, key=lambda leg: leg[0].account_id)


def _rejection_for(error: DomainError) -> tuple[CommandOutcome, int]:
    if isinstance(error, InsufficientFundsError):
        return CommandOutcome.INSUFFICIENT_FUNDS, 422
    if isinstance(error, AlreadyRevertedError):
        # A state conflict, not a malformed request (ADR-0015): the set was
        # reverted by someone else first. Same class as version_conflict.
        return CommandOutcome.DOMAIN_REJECTED, 409
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
