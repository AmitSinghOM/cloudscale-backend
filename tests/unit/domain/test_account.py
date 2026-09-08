"""Unit examples for the pure Account aggregate.

Validates: Requirements 1.1-1.7, 2.5-2.6.
"""

from dataclasses import FrozenInstanceError

import pytest

from cloudscale.domain.account import AccountState, apply, decide, fold
from cloudscale.domain.commands import MAX_SIGNED_BIGINT, Deposit, Withdraw
from cloudscale.domain.errors import (
    AccountIdentityMismatchError,
    AmountOutOfRangeError,
    InsufficientFundsError,
    InvalidAccountIdError,
    InvalidAccountStateError,
    InvalidAmountError,
    InvalidExpectedVersionError,
    UnknownCommandError,
    UnknownEventError,
    VersionOutOfRangeError,
)
from cloudscale.domain.events import Deposited, Withdrawn


def test_commands_and_events_are_immutable_values() -> None:
    command = Deposit(account_id="account-1", amount=10, expected_version=0)
    event = Deposited(account_id="account-1", amount=10)

    with pytest.raises(FrozenInstanceError):
        command.amount = 11  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        event.amount = 11  # type: ignore[misc]


def test_valid_commands_preserve_exact_values_and_bigint_boundaries() -> None:
    assert Deposit(" Account-A ", MAX_SIGNED_BIGINT, MAX_SIGNED_BIGINT) == Deposit(
        account_id=" Account-A ",
        amount=MAX_SIGNED_BIGINT,
        expected_version=MAX_SIGNED_BIGINT,
    )
    assert Withdraw("Account-A", 1, 0).account_id == "Account-A"


@pytest.mark.parametrize("account_id", ["", None, 123, False])
def test_commands_reject_invalid_account_ids(account_id: object) -> None:
    with pytest.raises(InvalidAccountIdError):
        Deposit(account_id=account_id, amount=1, expected_version=0)  # type: ignore[arg-type]


@pytest.mark.parametrize("amount", [True, False, 0, -1, 1.5, "1", None])
def test_commands_reject_non_positive_or_non_integer_amounts(
    amount: object,
) -> None:
    with pytest.raises(InvalidAmountError):
        Withdraw(account_id="account-1", amount=amount, expected_version=0)  # type: ignore[arg-type]


def test_commands_reject_amount_above_signed_bigint() -> None:
    with pytest.raises(AmountOutOfRangeError) as raised:
        Deposit("account-1", MAX_SIGNED_BIGINT + 1, 0)

    assert raised.value.code == "amount_out_of_range"


@pytest.mark.parametrize("expected_version", [True, False, -1, 1.5, "0", None])
def test_commands_reject_invalid_expected_versions(
    expected_version: object,
) -> None:
    with pytest.raises(InvalidExpectedVersionError):
        Deposit(
            account_id="account-1",
            amount=1,
            expected_version=expected_version,  # type: ignore[arg-type]
        )


def test_commands_reject_expected_version_above_signed_bigint() -> None:
    with pytest.raises(InvalidExpectedVersionError):
        Deposit("account-1", 1, MAX_SIGNED_BIGINT + 1)


def test_empty_fold_represents_an_unknown_account_at_zero_zero() -> None:
    assert fold(()) == AccountState(account_id=None, balance=0, version=0)


def test_unknown_account_cannot_have_nonzero_state() -> None:
    with pytest.raises(InvalidAccountStateError):
        AccountState(account_id=None, balance=1, version=0)


def test_deposit_decision_is_separate_from_projection_application() -> None:
    state = AccountState()
    command = Deposit("account-1", 25, expected_version=0)

    event = decide(state, command)

    assert event == Deposited("account-1", 25)
    assert state == AccountState()
    assert apply(state, event) == AccountState("account-1", balance=25, version=1)


def test_withdrawal_at_current_balance_is_accepted() -> None:
    state = AccountState("account-1", balance=25, version=1)

    event = decide(state, Withdraw("account-1", 25, expected_version=1))

    assert event == Withdrawn("account-1", 25)
    assert apply(state, event) == AccountState("account-1", balance=0, version=2)


def test_overdraft_is_rejected_without_mutating_state() -> None:
    state = AccountState("account-1", balance=25, version=1)

    with pytest.raises(InsufficientFundsError) as raised:
        decide(state, Withdraw("account-1", 26, expected_version=1))

    assert raised.value.code == "insufficient_funds"
    assert state == AccountState("account-1", balance=25, version=1)


def test_account_identity_is_case_and_whitespace_sensitive() -> None:
    state = AccountState("Account-1", balance=10, version=1)

    with pytest.raises(AccountIdentityMismatchError):
        decide(state, Deposit("account-1", 1, expected_version=1))
    with pytest.raises(AccountIdentityMismatchError):
        apply(state, Deposited("Account-1 ", 1))


def test_deposit_decision_and_apply_reject_balance_overflow() -> None:
    state = AccountState("account-1", balance=MAX_SIGNED_BIGINT, version=1)

    with pytest.raises(AmountOutOfRangeError):
        decide(state, Deposit("account-1", 1, expected_version=1))
    with pytest.raises(AmountOutOfRangeError):
        apply(state, Deposited("account-1", 1))


def test_decision_and_apply_reject_version_overflow() -> None:
    state = AccountState("account-1", balance=1, version=MAX_SIGNED_BIGINT)

    with pytest.raises(VersionOutOfRangeError):
        decide(state, Deposit("account-1", 1, expected_version=MAX_SIGNED_BIGINT))
    with pytest.raises(VersionOutOfRangeError):
        apply(state, Withdrawn("account-1", 1))


def test_fold_derives_balance_version_and_exact_identity() -> None:
    events = (
        Deposited(" account-1 ", 50),
        Withdrawn(" account-1 ", 20),
        Deposited(" account-1 ", 5),
    )

    assert fold(events) == AccountState(" account-1 ", balance=35, version=3)
    assert events[0] == Deposited(" account-1 ", 50)


def test_fold_can_continue_from_an_existing_state() -> None:
    initial = AccountState("account-1", balance=10, version=2)

    result = fold((Deposited("account-1", 3),), initial_state=initial)

    assert result == AccountState("account-1", balance=13, version=3)
    assert initial == AccountState("account-1", balance=10, version=2)


def test_unknown_command_and_event_types_have_typed_errors() -> None:
    with pytest.raises(UnknownCommandError):
        decide(AccountState(), object())  # type: ignore[arg-type]
    with pytest.raises(UnknownEventError):
        apply(AccountState(), object())  # type: ignore[arg-type]
