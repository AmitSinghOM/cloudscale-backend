"""Pure state transition and decision functions for the Account aggregate."""

from collections.abc import Iterable
from dataclasses import dataclass
from uuid import UUID

from .commands import MAX_SIGNED_BIGINT, AccountCommand, Deposit, Transfer, Withdraw
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

    Pure: both states are folded by the caller inside the transaction that
    will append both legs. The debit is checked against the source balance
    and the credit against the target's headroom, so applying either leg can
    never violate an aggregate invariant.
    """

    if not isinstance(command, Transfer):
        raise UnknownCommandError(f"unsupported command type: {type(command).__name__}")

    _require_matching_identity(source, command.account_id)
    _require_matching_identity(target, command.target_account_id)
    _require_next_version(source)
    _require_next_version(target)
    _require_funds(source, command.amount, "transfer")
    _require_credit_fits(target, command.amount, "transfer")

    debit = TransferDebited(
        account_id=command.account_id,
        amount=command.amount,
        transfer_id=transfer_id,
        counterparty=command.target_account_id,
    )
    credit = TransferCredited(
        account_id=command.target_account_id,
        amount=command.amount,
        transfer_id=transfer_id,
        counterparty=command.account_id,
    )
    return debit, credit


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
    "decide_transfer",
    "fold",
]
