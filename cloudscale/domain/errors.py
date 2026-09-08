"""Typed failures raised by the pure Account domain model."""


class DomainError(ValueError):
    """Base class for deterministic domain validation and decision failures."""

    code = "domain_error"


class InvalidAccountIdError(DomainError):
    """The account identifier is not an exact, non-empty string."""

    code = "invalid_account_id"


class InvalidAmountError(DomainError):
    """An amount is not a positive, non-Boolean integer."""

    code = "invalid_amount"


class InvalidExpectedVersionError(DomainError):
    """An expected version is not a non-negative, non-Boolean integer."""

    code = "invalid_expected_version"


class AmountOutOfRangeError(DomainError):
    """An amount or resulting balance cannot fit in a signed BIGINT."""

    code = "amount_out_of_range"


class VersionOutOfRangeError(DomainError):
    """A version cannot fit in the non-negative signed-BIGINT range."""

    code = "version_out_of_range"


class InvalidAccountStateError(DomainError):
    """An AccountState violates aggregate invariants."""

    code = "invalid_account_state"


class AccountIdentityMismatchError(DomainError):
    """A command or event targets a different exact account identifier."""

    code = "account_identity_mismatch"


class InsufficientFundsError(DomainError):
    """A withdrawal would make the account balance negative."""

    code = "insufficient_funds"


class UnknownCommandError(DomainError):
    """The aggregate received an unsupported command type."""

    code = "unknown_command"


class UnknownEventError(DomainError):
    """The aggregate received an unsupported event type."""

    code = "unknown_event"


__all__ = [
    "AccountIdentityMismatchError",
    "AmountOutOfRangeError",
    "DomainError",
    "InsufficientFundsError",
    "InvalidAccountIdError",
    "InvalidAccountStateError",
    "InvalidAmountError",
    "InvalidExpectedVersionError",
    "UnknownCommandError",
    "UnknownEventError",
    "VersionOutOfRangeError",
]
