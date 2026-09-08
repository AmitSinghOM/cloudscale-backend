"""Pure state transition and decision functions for the Account aggregate."""

from collections.abc import Iterable
from dataclasses import dataclass

from .commands import MAX_SIGNED_BIGINT, AccountCommand, Deposit, Withdraw
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
from .events import AccountEvent, Deposited, Withdrawn


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


def decide(state: AccountState, command: AccountCommand) -> AccountEvent:
    """Return the single event selected by a valid command without mutating state.

    Optimistic expected-version comparison belongs to the application/repository
    transaction. The command value validates that the supplied version is
    representable; this function makes only aggregate business decisions.
    """

    if not isinstance(command, (Deposit, Withdraw)):
        raise UnknownCommandError(f"unsupported command type: {type(command).__name__}")

    _require_matching_identity(state, command.account_id)
    _require_next_version(state)

    if isinstance(command, Deposit):
        if command.amount > MAX_SIGNED_BIGINT - state.balance:
            raise AmountOutOfRangeError(
                "deposit would move balance outside the signed-BIGINT range"
            )
        return Deposited(account_id=command.account_id, amount=command.amount)

    if command.amount > state.balance:
        raise InsufficientFundsError("withdrawal amount exceeds current balance")
    return Withdrawn(account_id=command.account_id, amount=command.amount)


def apply(state: AccountState, event: AccountEvent) -> AccountState:
    """Return a new state with one event applied; never mutate the input state."""

    if not isinstance(event, (Deposited, Withdrawn)):
        raise UnknownEventError(f"unsupported event type: {type(event).__name__}")

    _require_matching_identity(state, event.account_id)
    _require_next_version(state)

    if isinstance(event, Deposited):
        if event.amount > MAX_SIGNED_BIGINT - state.balance:
            raise AmountOutOfRangeError(
                "deposit would move balance outside the signed-BIGINT range"
            )
        balance = state.balance + event.amount
    else:
        if event.amount > state.balance:
            raise InsufficientFundsError(
                "withdrawal event would make account balance negative"
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


__all__ = ["AccountState", "apply", "decide", "fold"]
