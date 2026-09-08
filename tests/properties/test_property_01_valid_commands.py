"""Feature: cloudscale-production-readiness, Property 1.

Property 1: Valid commands map to one domain event.
Validates: Requirements 1.1, 1.2, 1.3.
"""

from hypothesis import given, settings, strategies as st

from cloudscale.domain.account import AccountState, decide
from cloudscale.domain.commands import MAX_SIGNED_BIGINT, Deposit, Withdraw
from cloudscale.domain.events import Deposited, Withdrawn

_ACCOUNT_IDS = st.text(min_size=1, max_size=64)
_APPENDABLE_VERSIONS = st.integers(min_value=0, max_value=MAX_SIGNED_BIGINT - 1)


@st.composite
def _accepted_deposit_cases(draw):
    account_id = draw(_ACCOUNT_IDS)
    if draw(st.booleans()):
        state = AccountState()
    else:
        balance = draw(st.integers(min_value=0, max_value=MAX_SIGNED_BIGINT - 1))
        state = AccountState(
            account_id=account_id,
            balance=balance,
            version=draw(_APPENDABLE_VERSIONS),
        )

    amount = draw(st.integers(min_value=1, max_value=MAX_SIGNED_BIGINT - state.balance))
    return state, account_id, amount


@st.composite
def _affordable_withdrawal_cases(draw):
    account_id = draw(_ACCOUNT_IDS)
    balance = draw(st.integers(min_value=1, max_value=MAX_SIGNED_BIGINT))
    state = AccountState(
        account_id=account_id,
        balance=balance,
        version=draw(_APPENDABLE_VERSIONS),
    )
    amount = draw(st.integers(min_value=1, max_value=balance))
    return state, account_id, amount


@settings(max_examples=100)
@given(case=_accepted_deposit_cases())
def test_accepted_deposit_produces_exactly_one_matching_event(
    case: tuple[AccountState, str, int],
) -> None:
    state, account_id, amount = case
    command = Deposit(
        account_id=account_id,
        amount=amount,
        expected_version=state.version,
    )

    events = (decide(state, command),)

    assert not isinstance(amount, bool)
    assert events == (Deposited(account_id=account_id, amount=amount),)


@settings(max_examples=100)
@given(case=_affordable_withdrawal_cases())
def test_affordable_withdrawal_produces_exactly_one_matching_event(
    case: tuple[AccountState, str, int],
) -> None:
    state, account_id, amount = case
    command = Withdraw(
        account_id=account_id,
        amount=amount,
        expected_version=state.version,
    )

    events = (decide(state, command),)

    assert not isinstance(amount, bool)
    assert events == (Withdrawn(account_id=account_id, amount=amount),)
