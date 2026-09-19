"""Pure state transition and decision functions for the Account aggregate."""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from .commands import (
    MAX_SIGNED_BIGINT,
    AccountCommand,
    Deposit,
    ExpireHold,
    Hold,
    Leg,
    Post,
    PostHold,
    Transfer,
    VoidHold,
    Withdraw,
)
from .errors import (
    AccountIdentityMismatchError,
    AmountOutOfRangeError,
    CaptureExceedsHoldError,
    HoldExpiredError,
    HoldNotExpiredError,
    HoldNotOpenError,
    InsufficientFundsError,
    InvalidAccountIdError,
    InvalidAccountStateError,
    UnknownCommandError,
    UnknownEventError,
    VersionOutOfRangeError,
)
from .events import (
    BALANCE_SIGN,
    HELD_SIGN,
    AccountEvent,
    Deposited,
    HoldPlaced,
    HoldPosted,
    HoldReleased,
    TransferCredited,
    TransferDebited,
    Withdrawn,
)

_EVENT_CLASSES = (
    Deposited,
    Withdrawn,
    TransferDebited,
    TransferCredited,
    HoldPlaced,
    HoldReleased,
    HoldPosted,
)

#: Shape-and-semantics version of :class:`AccountState` as folded by this build
#: (ADR-0012). Stream snapshots record it; a snapshot whose version differs is
#: discarded and the stream is refolded from the log. Bump it when the state's
#: fields change OR when an upcaster changes how an existing event folds.
CURRENT_STATE_VERSION = 2  # v2: ``held`` added (ADR-0014)


@dataclass(frozen=True, slots=True)
class AccountState:
    """State derived only by folding an account's ordered domain events.

    ``held`` is the sum of this account's open holds (ADR-0014);
    ``available`` is what a debit may use.
    """

    account_id: str | None = None
    balance: int = 0
    version: int = 0
    held: int = 0

    @property
    def available(self) -> int:
        return self.balance - self.held

    def __post_init__(self) -> None:
        if self.account_id is not None and (
            not isinstance(self.account_id, str) or self.account_id == ""
        ):
            raise InvalidAccountIdError("account_id must be None or a non-empty string")
        _require_non_negative_int(self.balance, "balance")
        if self.balance > MAX_SIGNED_BIGINT:
            raise AmountOutOfRangeError("balance exceeds the signed-BIGINT range")
        _require_non_negative_int(self.held, "held")
        if self.held > self.balance:
            raise InvalidAccountStateError("held funds cannot exceed the balance")
        _require_non_negative_int(self.version, "version")
        if self.version > MAX_SIGNED_BIGINT:
            raise VersionOutOfRangeError("version exceeds the signed-BIGINT range")
        if self.account_id is None and (self.balance != 0 or self.version != 0):
            raise InvalidAccountStateError(
                "an unknown account must have balance zero and version zero"
            )


def _require_non_negative_int(value: object, what: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise InvalidAccountStateError(
            f"{what} must be a non-negative, non-Boolean integer"
        )


def _require_matching_identity(state: AccountState, target_account_id: str) -> None:
    if state.account_id is not None and state.account_id != target_account_id:
        raise AccountIdentityMismatchError(
            "command or event account_id does not match AccountState exactly"
        )


def _require_next_version(state: AccountState) -> None:
    if state.version == MAX_SIGNED_BIGINT:
        raise VersionOutOfRangeError(
            "the next event version exceeds the signed-BIGINT range"
        )


def _require_credit_fits(state: AccountState, amount: int, what: str) -> None:
    if amount > MAX_SIGNED_BIGINT - state.balance:
        raise AmountOutOfRangeError(
            f"{what} would move balance outside the signed-BIGINT range"
        )


def _require_funds(state: AccountState, amount: int, what: str) -> None:
    """Debits are checked against AVAILABLE funds: balance minus open holds (ADR-0014)."""
    if amount > state.available:
        raise InsufficientFundsError(f"{what} amount exceeds available balance")


def decide(state: AccountState, command: AccountCommand) -> AccountEvent:
    """Return the single event selected by a valid command without mutating state.

    Optimistic expected-version comparison belongs to the application/repository
    transaction. The command value validates that the supplied version is
    representable; this function makes only aggregate business decisions.
    A :class:`Transfer` spans two aggregates and is decided by
    :func:`decide_transfer`.
    """

    if not isinstance(command, (Deposit, Withdraw)):
        raise UnknownCommandError(f"unsupported command type: {type(command).__name__}")

    _require_matching_identity(state, command.account_id)
    _require_next_version(state)

    if isinstance(command, Deposit):
        _require_credit_fits(state, command.amount, "deposit")
        return Deposited(account_id=command.account_id, amount=command.amount)

    _require_funds(state, command.amount, "withdrawal")
    return Withdrawn(account_id=command.account_id, amount=command.amount)


def decide_transfer(
    source: AccountState,
    target: AccountState,
    command: Transfer,
    *,
    transfer_id: UUID,
) -> tuple[TransferDebited, TransferCredited]:
    """Return the debit and credit legs of a valid transfer (ADR-0011).

    The two-leg case of :func:`decide_postings`; kept as a typed convenience
    so callers get the pair in (debit, credit) order.
    """

    if not isinstance(command, Transfer):
        raise UnknownCommandError(f"unsupported command type: {type(command).__name__}")
    states = {command.account_id: source, command.target_account_id: target}
    events = decide_postings(states, command, transfer_id=transfer_id)
    by_account = {event.account_id: event for event in events}
    debit = by_account[command.account_id]
    credit = by_account[command.target_account_id]
    if not isinstance(debit, TransferDebited) or not isinstance(
        credit, TransferCredited
    ):  # pragma: no cover - Transfer.legs() fixes the directions
        raise UnknownCommandError("transfer legs have unexpected directions")
    return debit, credit


def decide_postings(
    states: Mapping[str, AccountState],
    command: Transfer | Post,
    *,
    transfer_id: UUID,
) -> tuple[TransferDebited | TransferCredited, ...]:
    """Return one leg event per posting of a valid, balanced set (ADR-0013).

    Pure: every named stream's state is folded by the caller inside the
    transaction that will append every leg. Each debited account must have
    the funds and each credited account the headroom; one failing leg rejects
    the whole set. ``counterparty`` is the other account for two legs and
    the anchor (the payer) for more, except the anchor's own leg, which
    names its largest credited payee. Events are returned in the command's
    leg order; the caller sorts by account id before appending.
    """

    if not isinstance(command, (Transfer, Post)):
        raise UnknownCommandError(f"unsupported command type: {type(command).__name__}")
    legs = command.legs()
    for leg in legs:
        state = states[leg.account_id]
        _require_matching_identity(state, leg.account_id)
        _require_next_version(state)
        if leg.direction == "debit":
            _require_funds(state, leg.amount, "posting")
        else:
            _require_credit_fits(state, leg.amount, "posting")

    def counterparty_for(leg: Leg) -> str:
        if len(legs) == 2:
            return next(other.account_id for other in legs if other is not leg)
        if leg.account_id != command.account_id:
            return command.account_id  # every other leg points at the payer
        # The anchor's own leg names its primary payee: the largest credit,
        # first in leg order on ties. Deterministic, and what a statement shows.
        credits = [other for other in legs if other.direction == "credit"]
        return max(credits, key=lambda c: c.amount).account_id

    events: list[TransferDebited | TransferCredited] = []
    for leg in legs:
        kind = TransferDebited if leg.direction == "debit" else TransferCredited
        events.append(
            kind(
                account_id=leg.account_id,
                amount=leg.amount,
                transfer_id=transfer_id,
                counterparty=counterparty_for(leg),
            )
        )
    return tuple(events)


@dataclass(frozen=True, slots=True)
class OpenHold:
    """An open hold as derived from the source stream's own events (ADR-0014).

    Built by the storage from ``HoldPlaced`` minus any ``HoldPosted`` /
    ``HoldReleased`` for the same ``hold_id`` — inside the command's
    transaction, never from the eventual read model.
    """

    hold_id: UUID
    amount: int
    counterparty: str
    expires_at: str

    def expired_at(self, now: datetime) -> bool:
        return now >= datetime.fromisoformat(self.expires_at.replace("Z", "+00:00"))


def open_hold_from_events(
    hold_id: UUID, events: Iterable[HoldPlaced | HoldReleased | HoldPosted]
) -> OpenHold | None:
    """Derive the open hold for ``hold_id`` from that hold's events, in order."""
    placed: HoldPlaced | None = None
    for event in events:
        if event.hold_id != hold_id:
            continue
        if isinstance(event, HoldPlaced):
            placed = event
        elif isinstance(event, HoldPosted) or (
            isinstance(event, HoldReleased) and event.reason != "partial"
        ):
            return None  # settled or released: closed for good
    if placed is None:
        return None
    return OpenHold(hold_id, placed.amount, placed.counterparty, placed.expires_at)


def decide_hold(state: AccountState, command: Hold, *, hold_id: UUID) -> HoldPlaced:
    """Reserve funds: checked against AVAILABLE, not balance (ADR-0014)."""
    if not isinstance(command, Hold):
        raise UnknownCommandError(f"unsupported command type: {type(command).__name__}")
    _require_matching_identity(state, command.account_id)
    _require_next_version(state)
    _require_funds(state, command.amount, "hold")
    return HoldPlaced(
        account_id=command.account_id,
        amount=command.amount,
        hold_id=hold_id,
        counterparty=command.target_account_id,
        expires_at=command.expires_at,
    )


def decide_post_hold(
    source: AccountState,
    target: AccountState,
    hold: OpenHold | None,
    command: PostHold,
    *,
    now: datetime,
) -> tuple[AccountEvent, ...]:
    """Settle a hold: ``HoldPosted`` on the source, ``TransferCredited`` on the target,
    plus a partial ``HoldReleased`` when less than the held amount is captured.

    ``now`` is the decision clock (the same one that stamps ``occurred_at``);
    the fold never reads it — only the decision does, once, and the outcome
    is persisted.
    """
    if not isinstance(command, PostHold):
        raise UnknownCommandError(f"unsupported command type: {type(command).__name__}")
    if hold is None:
        raise HoldNotOpenError(
            f"hold {command.hold_id} is not open on {command.account_id}"
        )
    if hold.expired_at(now):
        raise HoldExpiredError(f"hold {command.hold_id} expired at {hold.expires_at}")
    amount = hold.amount if command.amount is None else command.amount
    if amount > hold.amount:
        raise CaptureExceedsHoldError(f"capture {amount} exceeds held {hold.amount}")
    _require_matching_identity(source, command.account_id)
    _require_matching_identity(target, hold.counterparty)
    _require_next_version(source)
    _require_next_version(target)
    if hold.amount > source.held or hold.amount > source.balance:
        raise InvalidAccountStateError("open hold exceeds the source's held funds")
    _require_credit_fits(target, amount, "hold posting")
    events: list[AccountEvent] = [
        HoldPosted(
            account_id=command.account_id,
            amount=amount,
            hold_id=command.hold_id,
            counterparty=hold.counterparty,
        ),
        TransferCredited(
            account_id=hold.counterparty,
            amount=amount,
            transfer_id=command.hold_id,
            counterparty=command.account_id,
        ),
    ]
    if amount < hold.amount:
        events.append(
            HoldReleased(
                account_id=command.account_id,
                amount=hold.amount - amount,
                hold_id=command.hold_id,
                reason="partial",
            )
        )
    return tuple(events)


def decide_release_hold(
    state: AccountState,
    hold: OpenHold | None,
    command: VoidHold | ExpireHold,
    *,
    now: datetime,
) -> HoldReleased:
    """Void (any time while open) or expire (only once ``expires_at`` has passed)."""
    if not isinstance(command, (VoidHold, ExpireHold)):
        raise UnknownCommandError(f"unsupported command type: {type(command).__name__}")
    if hold is None:
        raise HoldNotOpenError(
            f"hold {command.hold_id} is not open on {command.account_id}"
        )
    if isinstance(command, ExpireHold) and not hold.expired_at(now):
        raise HoldNotExpiredError(
            f"hold {command.hold_id} expires at {hold.expires_at}"
        )
    _require_matching_identity(state, command.account_id)
    _require_next_version(state)
    if hold.amount > state.held:
        raise InvalidAccountStateError("open hold exceeds the account's held funds")
    return HoldReleased(
        account_id=command.account_id,
        amount=hold.amount,
        hold_id=command.hold_id,
        reason="voided" if isinstance(command, VoidHold) else "expired",
    )


def apply(state: AccountState, event: AccountEvent) -> AccountState:
    """Return a new state with one event applied; never mutate the input state.

    ``BALANCE_SIGN`` and ``HELD_SIGN`` (``domain/events.py``) are the single
    tables every projection reads; this fold applies the same two signs and
    then enforces the aggregate invariants (non-negative balance, held within
    balance, BIGINT range).
    """

    if not isinstance(event, _EVENT_CLASSES):
        raise UnknownEventError(f"unsupported event type: {type(event).__name__}")
    event_type = type(event).__name__

    _require_matching_identity(state, event.account_id)
    _require_next_version(state)

    balance_delta = BALANCE_SIGN[event_type] * event.amount
    held_delta = HELD_SIGN[event_type] * event.amount
    if balance_delta > 0:
        _require_credit_fits(state, event.amount, "credit event")
    if state.balance + balance_delta < 0:
        raise InsufficientFundsError("debit event would make account balance negative")
    held = state.held + held_delta
    if held < 0:
        raise InvalidAccountStateError("release would make held funds negative")
    if held > state.balance + balance_delta:
        raise InvalidAccountStateError("hold would exceed the account balance")

    return AccountState(
        account_id=state.account_id or event.account_id,
        balance=state.balance + balance_delta,
        version=state.version + 1,
        held=held,
    )


def fold(
    events: Iterable[AccountEvent], initial_state: AccountState | None = None
) -> AccountState:
    """Derive account state from ordered events, starting from unknown ``0/0``."""

    state = AccountState() if initial_state is None else initial_state
    for event in events:
        state = apply(state, event)
    return state


__all__ = [
    "CURRENT_STATE_VERSION",
    "AccountState",
    "OpenHold",
    "apply",
    "decide",
    "decide_hold",
    "decide_post_hold",
    "decide_postings",
    "decide_release_hold",
    "decide_transfer",
    "fold",
    "open_hold_from_events",
]
