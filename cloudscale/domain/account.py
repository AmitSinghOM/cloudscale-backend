"""Pure state transition and decision functions for the Account aggregate."""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from uuid import UUID

from .commands import (
    MAX_SIGNED_BIGINT,
    AccountCommand,
    Deposit,
    Leg,
    Post,
    Transfer,
    Withdraw,
)
from .errors import (
    AccountIdentityMismatchError,
    AmountOutOfRangeError,
    InsufficientFundsError,
    InvalidAccountIdError,
    InvalidAccountStateError,
    UnknownCommandError,
    UnknownEventError,
    VersionOutOfRangeError,
)
from .events import (
    AccountEvent,
    Deposited,
    TransferCredited,
    TransferDebited,
    Withdrawn,
)

#: Shape-and-semantics version of :class:`AccountState` as folded by this build
#: (ADR-0012). Stream snapshots record it; a snapshot whose version differs is
#: discarded and the stream is refolded from the log. Bump it when the state's
#: fields change OR when an upcaster changes how an existing event folds.
CURRENT_STATE_VERSION = 1


@dataclass(frozen=True, slots=True)
class AccountState:
    """State derived only by folding an account's ordered domain events."""

    account_id: str | None = None
    balance: int = 0
    version: int = 0

    def __post_init__(self) -> None:
        if self.account_id is not None and (
            not isinstance(self.account_id, str) or self.account_id == ""
        ):
            raise InvalidAccountIdError("account_id must be None or a non-empty string")
        if (
            not isinstance(self.balance, int)
            or isinstance(self.balance, bool)
            or self.balance < 0
        ):
            raise InvalidAccountStateError(
                "balance must be a non-negative, non-Boolean integer"
            )
        if self.balance > MAX_SIGNED_BIGINT:
            raise AmountOutOfRangeError("balance exceeds the signed-BIGINT range")
        if (
            not isinstance(self.version, int)
            or isinstance(self.version, bool)
            or self.version < 0
        ):
            raise InvalidAccountStateError(
                "version must be a non-negative, non-Boolean integer"
            )
        if self.version > MAX_SIGNED_BIGINT:
            raise VersionOutOfRangeError("version exceeds the signed-BIGINT range")
        if self.account_id is None and (self.balance != 0 or self.version != 0):
            raise InvalidAccountStateError(
                "an unknown account must have balance zero and version zero"
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
    if amount > state.balance:
        raise InsufficientFundsError(f"{what} amount exceeds current balance")


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


def apply(state: AccountState, event: AccountEvent) -> AccountState:
    """Return a new state with one event applied; never mutate the input state."""

    if not isinstance(event, (Deposited, Withdrawn, TransferDebited, TransferCredited)):
        raise UnknownEventError(f"unsupported event type: {type(event).__name__}")

    _require_matching_identity(state, event.account_id)
    _require_next_version(state)

    if isinstance(event, (Deposited, TransferCredited)):
        _require_credit_fits(state, event.amount, "credit event")
        balance = state.balance + event.amount
    else:
        if event.amount > state.balance:
            raise InsufficientFundsError(
                "debit event would make account balance negative"
            )
        balance = state.balance - event.amount

    return AccountState(
        account_id=state.account_id or event.account_id,
        balance=balance,
        version=state.version + 1,
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
    "apply",
    "decide",
    "decide_postings",
    "decide_transfer",
    "fold",
]
