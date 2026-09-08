"""Feature: cloudscale-production-readiness, Property 4.

Property 4: Concurrent command histories cannot overdraw an account.
Validates: Requirements 2.5, 2.6.
"""

from hypothesis import given, settings, strategies as st

from cloudscale.domain.account import apply, decide, fold
from cloudscale.domain.commands import AccountCommand, Deposit, Withdraw
from cloudscale.domain.errors import InsufficientFundsError
from cloudscale.domain.events import AccountEvent, Deposited, Withdrawn

_ACCOUNT_IDS = st.text(min_size=1, max_size=64)
_AMOUNTS = st.integers(min_value=1, max_value=50_000)


@st.composite
def _concurrent_histories(
    draw: st.DrawFn,
) -> tuple[
    tuple[AccountEvent, ...],
    tuple[tuple[AccountCommand, ...], ...],
]:
    """Generate a valid starting stream and ordered waves of competing commands."""
    account_id = draw(_ACCOUNT_IDS)
    starting_operations = draw(
        st.lists(
            st.tuples(st.booleans(), st.integers(min_value=1, max_value=10_000)),
            max_size=20,
        )
    )

    starting_events: list[AccountEvent] = []
    starting_balance = 0
    for is_deposit, candidate_amount in starting_operations:
        if is_deposit or starting_balance == 0:
            starting_events.append(Deposited(account_id, candidate_amount))
            starting_balance += candidate_amount
        else:
            amount = min(candidate_amount, starting_balance)
            starting_events.append(Withdrawn(account_id, amount))
            starting_balance -= amount

    starting_version = len(starting_events)
    wave_count = draw(st.integers(min_value=1, max_value=12))
    waves: list[tuple[AccountCommand, ...]] = []

    for wave_index in range(wave_count):
        expected_version = starting_version + wave_index
        deposit_amount = draw(_AMOUNTS)
        withdrawal_amount = (
            max(1, starting_balance) if wave_index == 0 else draw(_AMOUNTS)
        )
        contenders: list[tuple[bool, int]] = [
            (False, withdrawal_amount),
            (False, withdrawal_amount),
            (True, deposit_amount),
        ]
        contenders.extend(
            draw(
                st.lists(
                    st.tuples(st.booleans(), _AMOUNTS),
                    max_size=4,
                )
            )
        )
        order = draw(st.permutations(tuple(range(len(contenders)))))
        wave: list[AccountCommand] = []
        for contender_index in order:
            is_deposit, amount = contenders[contender_index]
            command_type = Deposit if is_deposit else Withdraw
            wave.append(command_type(account_id, amount, expected_version))
        waves.append(tuple(wave))

    return tuple(starting_events), tuple(waves)


@settings(max_examples=100)
@given(history=_concurrent_histories())
def test_concurrent_command_histories_cannot_overdraw_account(
    history: tuple[
        tuple[AccountEvent, ...],
        tuple[tuple[AccountCommand, ...], ...],
    ],
) -> None:
    """Serialize one winner per expected version and verify every committed prefix."""
    starting_events, competing_waves = history
    starting_state = fold(starting_events)
    state = starting_state
    committed_events = list(starting_events)
    accepted_deposits = 0
    accepted_withdrawals = 0

    for wave in competing_waves:
        wave_expected_version = wave[0].expected_version
        accepted_in_wave = 0

        for command in wave:
            state_before = state
            committed_count_before = len(committed_events)

            if command.expected_version != state.version:
                assert state == state_before
                assert len(committed_events) == committed_count_before
                continue

            try:
                event = decide(state, command)
            except InsufficientFundsError:
                assert state == state_before
                assert len(committed_events) == committed_count_before
                continue

            if isinstance(event, Withdrawn):
                assert event.amount <= state.balance
                assert (
                    accepted_withdrawals + event.amount
                    <= starting_state.balance + accepted_deposits
                )
                accepted_withdrawals += event.amount
            else:
                assert isinstance(event, Deposited)
                accepted_deposits += event.amount

            state = apply(state, event)
            committed_events.append(event)
            accepted_in_wave += 1

            assert command.expected_version == state_before.version
            assert state.version == state_before.version + 1
            assert state.balance >= 0
            assert fold(committed_events) == state

        assert accepted_in_wave == 1
        assert state.version == wave_expected_version + 1

    authoritative_state = fold(committed_events)

    assert authoritative_state == state
    assert authoritative_state.version == len(committed_events)
    assert authoritative_state.balance >= 0
    assert accepted_withdrawals <= starting_state.balance + accepted_deposits
