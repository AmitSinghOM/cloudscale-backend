"""Property tests for non-mutating invalid Account commands."""

import json
from collections.abc import Sequence

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cloudscale.domain.account import decide, fold
from cloudscale.domain.commands import Deposit, Withdraw
from cloudscale.domain.errors import DomainError
from cloudscale.domain.events import AccountEvent, Deposited, Withdrawn


@st.composite
def valid_streams(
    draw: st.DrawFn,
) -> tuple[str, tuple[AccountEvent, ...]]:
    """Build valid histories containing affordable withdrawals only."""
    account_id = draw(st.text(min_size=1, max_size=20))
    operations = draw(
        st.lists(
            st.tuples(
                st.booleans(),
                st.integers(min_value=1, max_value=10_000),
            ),
            max_size=20,
        )
    )

    balance = 0
    events: list[AccountEvent] = []
    for is_deposit, candidate_amount in operations:
        if is_deposit or balance == 0:
            events.append(Deposited(account_id, candidate_amount))
            balance += candidate_amount
        else:
            amount = min(candidate_amount, balance)
            events.append(Withdrawn(account_id, amount))
            balance -= amount

    return account_id, tuple(events)


invalid_account_ids = st.one_of(
    st.just(""),
    st.none(),
    st.booleans(),
    st.integers(),
    st.binary(max_size=8),
    st.lists(st.integers(), max_size=3),
)
invalid_amounts = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(max_value=0),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(max_size=8),
    st.binary(max_size=8),
)
unknown_commands = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    st.text(max_size=8),
    st.lists(st.integers(), max_size=3),
    st.dictionaries(st.text(max_size=4), st.integers(), max_size=3),
)
invalid_cases = st.one_of(
    st.tuples(st.just("unknown"), unknown_commands, st.booleans()),
    st.tuples(st.just("invalid_account_id"), invalid_account_ids, st.booleans()),
    st.tuples(st.just("invalid_amount"), invalid_amounts, st.booleans()),
    st.tuples(
        st.just("overdraft"),
        st.integers(min_value=1, max_value=10_000),
        st.just(True),
    ),
)


def _stream_bytes(stream: Sequence[AccountEvent]) -> bytes:
    """Serialize a modeled stream deterministically for byte-level comparison."""
    records = [
        {
            "account_id": event.account_id,
            "amount": event.amount,
            "event_type": type(event).__name__,
        }
        for event in stream
    ]
    return json.dumps(
        records,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


@settings(max_examples=100)
@given(modeled_stream=valid_streams(), invalid_case=invalid_cases)
def test_invalid_or_unaffordable_commands_do_not_mutate_stream(
    modeled_stream: tuple[str, tuple[AccountEvent, ...]],
    invalid_case: tuple[str, object, bool],
) -> None:
    """Feature: cloudscale-production-readiness, Property 2: Invalid or unaffordable commands do not mutate the stream.

    Validates: Requirements 1.4, 1.5
    """
    account_id, initial_events = modeled_stream
    stream = list(initial_events)
    state_before = fold(stream)
    bytes_before = _stream_bytes(stream)
    category, invalid_value, use_withdrawal = invalid_case
    command_type = Withdraw if use_withdrawal else Deposit

    with pytest.raises(DomainError):
        if category == "unknown":
            command = invalid_value
        elif category == "invalid_account_id":
            command = command_type(
                account_id=invalid_value,
                amount=1,
                expected_version=state_before.version,
            )
        elif category == "invalid_amount":
            command = command_type(
                account_id=account_id,
                amount=invalid_value,
                expected_version=state_before.version,
            )
        else:
            command = Withdraw(
                account_id=account_id,
                amount=state_before.balance + int(invalid_value),
                expected_version=state_before.version,
            )

        event = decide(state_before, command)  # type: ignore[arg-type]
        stream.append(event)

    assert _stream_bytes(stream) == bytes_before
    assert fold(stream) == state_before
    assert tuple(stream) == initial_events
