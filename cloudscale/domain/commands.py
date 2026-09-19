"""Immutable commands accepted by the Account aggregate."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal
from uuid import UUID

from .errors import (
    AmountOutOfRangeError,
    AnchorNotDebitedError,
    DuplicateAccountError,
    InvalidAccountIdError,
    InvalidAmountError,
    InvalidExpectedVersionError,
    InvalidExpiryError,
    SameAccountError,
    TooManyLegsError,
    UnbalancedPostingError,
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


@dataclass(frozen=True, slots=True)
class Transfer:
    """Request to move positive minor units from one account to another (ADR-0011).

    ``account_id`` is the source — the stream whose funds are at risk and the
    only one whose ``expected_version`` the caller supplies. The target is
    guarded by the storage's ``UNIQUE (stream, seq)`` inside the same
    transaction. A transfer is the two-leg case of :class:`Post` (ADR-0013);
    :meth:`legs` gives that view so one decision function serves both.
    """

    account_id: str
    target_account_id: str
    amount: int
    expected_version: int

    def __post_init__(self) -> None:
        _validate_account_id(self.account_id)
        _validate_account_id(self.target_account_id)
        if self.account_id == self.target_account_id:
            raise SameAccountError("a transfer needs two different accounts")
        _validate_amount(self.amount)
        _validate_expected_version(self.expected_version)

    def legs(self) -> tuple[Leg, Leg]:
        return (
            Leg(self.account_id, self.amount, "debit"),
            Leg(self.target_account_id, self.amount, "credit"),
        )


#: Public limit on legs per posting set (ADR-0013). Documented in API_ERRORS.
MAX_LEGS = 16

Direction = Literal["debit", "credit"]


@dataclass(frozen=True, slots=True)
class Leg:
    """One leg of a posting set: which account, how much, which way (ADR-0013)."""

    account_id: str
    amount: int
    direction: Direction

    def __post_init__(self) -> None:
        _validate_account_id(self.account_id)
        _validate_amount(self.amount)
        if self.direction not in ("debit", "credit"):
            raise InvalidAmountError("direction must be 'debit' or 'credit'")


@dataclass(frozen=True, slots=True)
class Post:
    """Request to commit a balanced set of 2..MAX_LEGS postings atomically (ADR-0013).

    ``account_id`` is the anchor: the caller's authorized account, the only
    one whose ``expected_version`` is supplied, and it must be debited — money
    leaves the anchor, so a caller cannot move funds between accounts it does
    not own by naming its own account as a decorative leg.
    """

    account_id: str
    postings: tuple[Leg, ...]
    expected_version: int

    def __post_init__(self) -> None:
        _validate_account_id(self.account_id)
        _validate_expected_version(self.expected_version)
        if not isinstance(self.postings, tuple):
            object.__setattr__(self, "postings", tuple(self.postings))
        if len(self.postings) < 2:
            raise UnbalancedPostingError("a posting set needs at least two legs")
        if len(self.postings) > MAX_LEGS:
            raise TooManyLegsError(f"a posting set may have at most {MAX_LEGS} legs")
        accounts = [leg.account_id for leg in self.postings]
        if len(set(accounts)) != len(accounts):
            raise DuplicateAccountError("each account may appear in one leg only")
        debits = sum(leg.amount for leg in self.postings if leg.direction == "debit")
        credits = sum(leg.amount for leg in self.postings if leg.direction == "credit")
        if debits != credits:
            raise UnbalancedPostingError(
                f"debits {debits} != credits {credits}; a posting set must balance"
            )
        if not any(
            leg.account_id == self.account_id and leg.direction == "debit"
            for leg in self.postings
        ):
            raise AnchorNotDebitedError("the anchor account must be a debited leg")

    def legs(self) -> tuple[Leg, ...]:
        return self.postings


def _validate_expires_at(value: object) -> None:
    if not isinstance(value, str) or not value:
        raise InvalidExpiryError("expires_at must be a UTC ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise InvalidExpiryError("expires_at must be a UTC ISO-8601 string") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise InvalidExpiryError("expires_at must be UTC")


def _validate_uuid(value: object, field_name: str) -> None:
    if not isinstance(value, UUID):
        raise InvalidAccountIdError(f"{field_name} must be a UUID")


@dataclass(frozen=True, slots=True)
class Hold:
    """Reserve ``amount`` on ``account_id`` for a later posting to the target (ADR-0014).

    ``expires_at`` is absolute UTC text: the HTTP layer converts a TTL using
    its clock so the domain never reads one. The hold id is the command id.
    """

    account_id: str
    target_account_id: str
    amount: int
    expected_version: int
    expires_at: str

    def __post_init__(self) -> None:
        _validate_account_id(self.account_id)
        _validate_account_id(self.target_account_id)
        if self.account_id == self.target_account_id:
            raise SameAccountError("a hold needs two different accounts")
        _validate_amount(self.amount)
        _validate_expected_version(self.expected_version)
        _validate_expires_at(self.expires_at)


@dataclass(frozen=True, slots=True)
class PostHold:
    """Settle an open hold: debit the reserved funds and credit the target (ADR-0014).

    ``amount`` defaults to the full held amount; a smaller capture releases
    the remainder in the same transaction.
    """

    account_id: str
    hold_id: UUID
    expected_version: int
    amount: int | None = None

    def __post_init__(self) -> None:
        _validate_account_id(self.account_id)
        _validate_uuid(self.hold_id, "hold_id")
        _validate_expected_version(self.expected_version)
        if self.amount is not None:
            _validate_amount(self.amount)


@dataclass(frozen=True, slots=True)
class VoidHold:
    """Release an open hold without moving funds (the payer cancelled)."""

    account_id: str
    hold_id: UUID
    expected_version: int

    def __post_init__(self) -> None:
        _validate_account_id(self.account_id)
        _validate_uuid(self.hold_id, "hold_id")
        _validate_expected_version(self.expected_version)


@dataclass(frozen=True, slots=True)
class ExpireHold:
    """Release an open hold whose ``expires_at`` has passed (the sweeper's command)."""

    account_id: str
    hold_id: UUID
    expected_version: int

    def __post_init__(self) -> None:
        _validate_account_id(self.account_id)
        _validate_uuid(self.hold_id, "hold_id")
        _validate_expected_version(self.expected_version)


HoldCommand = Hold | PostHold | VoidHold | ExpireHold
AccountCommand = (
    Deposit | Withdraw | Transfer | Post | Hold | PostHold | VoidHold | ExpireHold
)

__all__ = [
    "MAX_LEGS",
    "AccountCommand",
    "Deposit",
    "Direction",
    "ExpireHold",
    "Hold",
    "HoldCommand",
    "Leg",
    "MAX_SIGNED_BIGINT",
    "Post",
    "PostHold",
    "Transfer",
    "VoidHold",
    "Withdraw",
]
