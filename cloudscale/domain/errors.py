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


class SameAccountError(DomainError):
    """A transfer names the same account as source and target."""

    code = "same_account"


class UnbalancedPostingError(DomainError):
    """A posting set's debits and credits do not sum to the same amount (ADR-0013)."""

    code = "unbalanced"


class DuplicateAccountError(DomainError):
    """A posting set names the same account in more than one leg (ADR-0013)."""

    code = "duplicate_account"


class TooManyLegsError(DomainError):
    """A posting set has more legs than ``MAX_LEGS`` (ADR-0013)."""

    code = "too_many_legs"


class AnchorNotDebitedError(DomainError):
    """The anchor (authorized, version-guarded) account is not debited (ADR-0013)."""

    code = "anchor_not_debited"


class HoldNotOpenError(DomainError):
    """The named hold does not exist on this stream or is no longer open (ADR-0014)."""

    code = "hold_not_open"


class HoldExpiredError(DomainError):
    """The hold's ``expires_at`` has passed; it can only be expired or voided (ADR-0014)."""

    code = "hold_expired"


class HoldNotExpiredError(DomainError):
    """``ExpireHold`` before ``expires_at``; use ``VoidHold`` instead (ADR-0014)."""

    code = "hold_not_expired"


class CaptureExceedsHoldError(DomainError):
    """``PostHold.amount`` is greater than the amount held (ADR-0014)."""

    code = "capture_exceeds_hold"


class InvalidExpiryError(DomainError):
    """``expires_at`` is not a UTC ISO-8601 timestamp (ADR-0014)."""

    code = "invalid_expiry"


class NotRevertibleError(DomainError):
    """``transfer_id`` names nothing that moved money as a grouped set (ADR-0015)."""

    code = "not_revertible"


class AlreadyRevertedError(DomainError):
    """A reversal naming this ``transfer_id`` is already in the log (ADR-0015)."""

    code = "already_reverted"


class AnchorNotCreditedError(DomainError):
    """The revert's anchor is not an account the revert credits (ADR-0015)."""

    code = "anchor_not_credited"


class UnknownCommandError(DomainError):
    """The aggregate received an unsupported command type."""

    code = "unknown_command"


class UnknownEventError(DomainError):
    """The aggregate received an unsupported event type."""

    code = "unknown_event"


__all__ = [
    "AccountIdentityMismatchError",
    "AlreadyRevertedError",
    "AmountOutOfRangeError",
    "AnchorNotCreditedError",
    "AnchorNotDebitedError",
    "CaptureExceedsHoldError",
    "DomainError",
    "DuplicateAccountError",
    "HoldExpiredError",
    "HoldNotExpiredError",
    "HoldNotOpenError",
    "InsufficientFundsError",
    "InvalidAccountIdError",
    "InvalidAccountStateError",
    "InvalidAmountError",
    "InvalidExpectedVersionError",
    "InvalidExpiryError",
    "NotRevertibleError",
    "SameAccountError",
    "TooManyLegsError",
    "UnbalancedPostingError",
    "UnknownCommandError",
    "UnknownEventError",
    "VersionOutOfRangeError",
]
