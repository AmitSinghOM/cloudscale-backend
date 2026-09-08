"""Immutable commands accepted by the Account aggregate."""

from dataclasses import dataclass

from .errors import (
    AmountOutOfRangeError,
    InvalidAccountIdError,
    InvalidAmountError,
    InvalidExpectedVersionError,
)

MAX_SIGNED_BIGINT = 2**63 - 1


def _validate_account_id(account_id: object) -> None:
    if not isinstance(account_id, str) or account_id == "":
        raise InvalidAccountIdError("account_id must be a non-empty string")


def _validate_amount(amount: object) -> None:
    if not isinstance(amount, int) or isinstance(amount, bool) or amount <= 0:
        raise InvalidAmountError(
            "amount must be a positive, non-Boolean integer in minor units"
        )
    if amount > MAX_SIGNED_BIGINT:
        raise AmountOutOfRangeError("amount exceeds the signed-BIGINT range")


def _validate_expected_version(expected_version: object) -> None:
    if (
        not isinstance(expected_version, int)
        or isinstance(expected_version, bool)
        or expected_version < 0
        or expected_version > MAX_SIGNED_BIGINT
    ):
        raise InvalidExpectedVersionError(
            "expected_version must be a non-negative signed-BIGINT integer"
        )


@dataclass(frozen=True, slots=True)
class Deposit:
    """Request to add positive minor units to one exact account."""

    account_id: str
    amount: int
    expected_version: int

    def __post_init__(self) -> None:
        _validate_account_id(self.account_id)
        _validate_amount(self.amount)
        _validate_expected_version(self.expected_version)


@dataclass(frozen=True, slots=True)
class Withdraw:
    """Request to remove positive minor units from one exact account."""

    account_id: str
    amount: int
    expected_version: int

    def __post_init__(self) -> None:
        _validate_account_id(self.account_id)
        _validate_amount(self.amount)
        _validate_expected_version(self.expected_version)


AccountCommand = Deposit | Withdraw

__all__ = ["AccountCommand", "Deposit", "MAX_SIGNED_BIGINT", "Withdraw"]
