"""Double-entry Transfer: decision rules and the conservation invariant (ADR-0011)."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cloudscale.domain.account import (
    AccountState,
    apply,
    decide,
    decide_transfer,
    fold,
)
from cloudscale.domain.commands import MAX_SIGNED_BIGINT, Deposit, Transfer, Withdraw
from cloudscale.domain.errors import (
    AccountIdentityMismatchError,
    AmountOutOfRangeError,
    InsufficientFundsError,
    SameAccountError,
    UnknownCommandError,
)
from cloudscale.domain.events import (
    BALANCE_SIGN,
    HELD_SIGN,
    Deposited,
    EventEnvelope,
    TransferCredited,
    TransferDebited,
)
from cloudscale.domain.results import CommandOutcome, CommandResult, Posting
from cloudscale.domain.upcasting import CURRENT_SCHEMA_VERSION

TID = UUID("0b8e6f2c-9d4a-4b7e-8c1f-2a3b4c5d6e7f")


def _state(account_id: str, balance: int, version: int) -> AccountState:
    return AccountState(account_id=account_id, balance=balance, version=version)


# -- command value -------------------------------------------------------------------


def test_transfer_rejects_the_same_account_twice() -> None:
    with pytest.raises(SameAccountError) as excinfo:
        Transfer("a", "a", 5, 0)
    assert excinfo.value.code == "same_account"


def test_transfer_validates_both_ids_amount_and_version() -> None:
    with pytest.raises(ValueError):
        Transfer("", "b", 5, 0)
    with pytest.raises(ValueError):
        Transfer("a", "", 5, 0)
    with pytest.raises(ValueError):
        Transfer("a", "b", 0, 0)
    with pytest.raises(ValueError):
        Transfer("a", "b", 5, -1)


# -- decision ------------------------------------------------------------------------


def test_decide_transfer_returns_paired_legs() -> None:
    debit, credit = decide_transfer(
        _state("a", 100, 3),
        _state("b", 7, 1),
        Transfer("a", "b", 40, 3),
        transfer_id=TID,
    )
    assert debit == TransferDebited("a", 40, TID, counterparty="b")
    assert credit == TransferCredited("b", 40, TID, counterparty="a")


def test_decide_transfer_into_an_unknown_account_creates_it() -> None:
    debit, credit = decide_transfer(
        _state("a", 10, 1), AccountState(), Transfer("a", "new", 10, 1), transfer_id=TID
    )
    assert credit.account_id == "new"
    assert apply(AccountState(), credit) == _state("new", 10, 1)


def test_decide_transfer_refuses_insufficient_source_funds() -> None:
    with pytest.raises(InsufficientFundsError):
        decide_transfer(
            _state("a", 39, 1),
            _state("b", 0, 0),
            Transfer("a", "b", 40, 1),
            transfer_id=TID,
        )


def test_decide_transfer_refuses_a_credit_that_overflows_the_target() -> None:
    with pytest.raises(AmountOutOfRangeError):
        decide_transfer(
            _state("a", 5, 1),
            _state("b", MAX_SIGNED_BIGINT, 1),
            Transfer("a", "b", 1, 1),
            transfer_id=TID,
        )


def test_decide_transfer_checks_both_identities() -> None:
    with pytest.raises(AccountIdentityMismatchError):
        decide_transfer(
            _state("x", 5, 1),
            _state("b", 0, 0),
            Transfer("a", "b", 1, 1),
            transfer_id=TID,
        )
    with pytest.raises(AccountIdentityMismatchError):
        decide_transfer(
            _state("a", 5, 1),
            _state("y", 0, 0),
            Transfer("a", "b", 1, 1),
            transfer_id=TID,
        )


def test_single_aggregate_decide_does_not_accept_a_transfer() -> None:
    with pytest.raises(UnknownCommandError):
        decide(_state("a", 5, 1), Transfer("a", "b", 1, 1))


def test_decide_transfer_rejects_other_commands() -> None:
    with pytest.raises(UnknownCommandError):
        decide_transfer(
            _state("a", 5, 1),
            _state("b", 0, 0),
            Deposit("a", 1, 1),
            transfer_id=TID,  # type: ignore[arg-type]
        )


# -- apply ---------------------------------------------------------------------------


def test_apply_debit_and_credit_move_the_balance_in_opposite_directions() -> None:
    assert apply(_state("a", 100, 1), TransferDebited("a", 40, TID, "b")) == _state(
        "a", 60, 2
    )
    assert apply(_state("b", 1, 1), TransferCredited("b", 40, TID, "a")) == _state(
        "b", 41, 2
    )


def test_apply_debit_never_goes_negative() -> None:
    with pytest.raises(InsufficientFundsError):
        apply(_state("a", 10, 1), TransferDebited("a", 11, TID, "b"))


def test_balance_sign_table_covers_every_event_type() -> None:
    assert dict(BALANCE_SIGN) == {
        "Deposited": 1,
        "Withdrawn": -1,
        "TransferCredited": 1,
        "TransferDebited": -1,
        "HoldPlaced": 0,
        "HoldReleased": 0,
        "HoldPosted": -1,
        "ReversalDebited": -1,
        "ReversalCredited": 1,
    }
    # Every event type has exactly one entry in each sign table (ADR-0014).
    assert set(HELD_SIGN) == set(BALANCE_SIGN) == set(CURRENT_SCHEMA_VERSION)


# -- envelope ------------------------------------------------------------------------


def test_transfer_leg_envelope_round_trips_through_json() -> None:
    leg = TransferDebited("a", 40, TID, "b")
    envelope = EventEnvelope.from_domain_event(
        leg,
        event_id=uuid4(),
        stream_version=4,
        occurred_at=datetime.now(UTC),
        correlation_id=uuid4(),
        causation_id=TID,
        command_id=TID,
    )
    assert envelope.event_type == "TransferDebited"
    assert envelope.schema_version == 1
    restored = EventEnvelope.from_json(envelope.to_json())
    assert restored == envelope
    assert restored.to_domain_event() == leg


def test_transfer_leg_envelope_requires_the_pairing_fields() -> None:
    envelope = EventEnvelope.from_domain_event(
        TransferCredited("b", 1, TID, "a"),
        event_id=uuid4(),
        stream_version=1,
        occurred_at=datetime.now(UTC),
        correlation_id=uuid4(),
        causation_id=TID,
        command_id=TID,
    )
    broken = envelope.to_dict()
    del broken["payload"]["transfer_id"]  # type: ignore[index]
    with pytest.raises(ValueError, match="transfer payload"):
        EventEnvelope.from_dict(broken)


# -- result postings -----------------------------------------------------------------


def _accepted(postings: tuple[Posting, ...] = ()) -> CommandResult:
    event_id = uuid4()
    return CommandResult(
        command_id=uuid4(),
        request_hash=b"\x00" * 32,
        outcome=CommandOutcome.ACCEPTED,
        account_id="a",
        expected_version=1,
        current_version=1,
        committed_version=2,
        event_id=event_id,
        correlation_id=uuid4(),
        error_code=None,
        http_status=201,
        created_at=datetime.now(UTC),
        postings=postings or (Posting("a", event_id, 2),),
    )


def test_single_posting_is_derived_and_survives_json() -> None:
    result = _accepted()
    assert result.postings == (Posting("a", result.event_id, 2),)  # type: ignore[arg-type]
    assert CommandResult.from_json(result.to_json()) == result


def test_results_persisted_before_postings_existed_still_load() -> None:
    legacy = _accepted().to_dict()
    del legacy["postings"]
    restored = CommandResult.from_dict(legacy)
    assert len(restored.postings) == 1
    assert restored.postings[0].account_id == "a"


def test_postings_must_include_the_addressed_account() -> None:
    with pytest.raises(ValueError, match="addressed account"):
        _accepted((Posting("b", uuid4(), 1),))


# -- conservation property -----------------------------------------------------------

_ACCOUNTS = ("a", "b", "c")


_STEPS = st.lists(
    st.tuples(
        st.sampled_from(("deposit", "withdraw", "transfer")),
        st.sampled_from(_ACCOUNTS),
        st.integers(1, 500),
    ),
    max_size=40,
)


def _apply_step(
    logs: dict[str, list], kind: str, acct: str, other: str, amount: int
) -> int:
    """Apply one random step to the logs; return the cash that entered (+) or left (-)."""
    states = {name: fold(events) for name, events in logs.items()}
    version = states[acct].version
    try:
        if kind == "transfer":
            if other == acct:
                return 0
            debit, credit = decide_transfer(
                states[acct],
                states[other],
                Transfer(acct, other, amount, version),
                transfer_id=uuid4(),
            )
            logs[acct].append(debit)
            logs[other].append(credit)
            return 0
        command = (
            Deposit(acct, amount, version)
            if kind == "deposit"
            else Withdraw(acct, amount, version)
        )
        event = decide(states[acct], command)
    except (InsufficientFundsError, AmountOutOfRangeError):
        return 0  # a rejected step changes nothing
    logs[acct].append(event)
    return amount if isinstance(event, Deposited) else -amount


def _transfer_ids(logs: dict[str, list], leg: type) -> set:
    return {
        e.transfer_id for events in logs.values() for e in events if isinstance(e, leg)
    }


@settings(max_examples=150, deadline=None)
@given(
    steps=_STEPS,
    targets=st.lists(st.sampled_from(_ACCOUNTS), min_size=40, max_size=40),
)
def test_transfers_conserve_the_total_and_never_go_negative(
    steps: list[tuple[str, str, int]], targets: list[str]
) -> None:
    """Sum of balances changes only by deposits and withdrawals, never by transfers."""
    logs: dict[str, list] = {name: [] for name in _ACCOUNTS}
    external = sum(
        _apply_step(logs, kind, acct, targets[index], amount)
        for index, (kind, acct, amount) in enumerate(steps)
    )

    final = {name: fold(events) for name, events in logs.items()}
    assert sum(state.balance for state in final.values()) == external
    assert all(state.balance >= 0 for state in final.values())
    # Every debit leg has exactly one credit leg with the same transfer id.
    assert _transfer_ids(logs, TransferDebited) == _transfer_ids(logs, TransferCredited)
