"""ADR-0015: a revert is the mirror of a committed posting set.

The algebra to lock: applying a set and then its revert leaves every balance
where it started; reverting the revert restores the set's effect; nothing
ever goes negative or creates money. Everything the command refuses is
refused from the log-derived inputs, never from a clock or a read model.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cloudscale.domain.account import (
    MOVEMENT_LEGS,
    AccountState,
    apply,
    decide_postings,
    decide_revert,
    fold,
)
from cloudscale.domain.commands import Leg, Post, Revert, Transfer
from cloudscale.domain.errors import (
    AlreadyRevertedError,
    AnchorNotCreditedError,
    InsufficientFundsError,
    NotRevertibleError,
    UnknownCommandError,
)
from cloudscale.domain.events import (
    Deposited,
    EventEnvelope,
    HoldPlaced,
    HoldPosted,
    HoldReleased,
    ReversalCredited,
    ReversalDebited,
    TransferCredited,
)


def _states(**balances: int) -> dict[str, AccountState]:
    return {name: AccountState(name, balance, 1) for name, balance in balances.items()}


def _apply_all(states: dict[str, AccountState], events) -> dict[str, AccountState]:
    out = dict(states)
    for event in events:
        out[event.account_id] = apply(out[event.account_id], event)
    return out


# -- mirror derivation ----------------------------------------------------------------


def test_two_leg_transfer_mirrors_into_debit_of_payee_and_credit_of_payer() -> None:
    original_id, revert_id = uuid4(), uuid4()
    original = decide_postings(
        _states(a=100, b=0), Transfer("a", "b", 40, 1), transfer_id=original_id
    )
    after = _apply_all(_states(a=100, b=0), original)
    mirror = decide_revert(
        after,
        original,
        Revert("a", original_id, 2),
        transfer_id=revert_id,
        reverted_by=None,
    )
    by_account = {e.account_id: e for e in mirror}
    assert isinstance(by_account["b"], ReversalDebited) and by_account["b"].amount == 40
    assert (
        isinstance(by_account["a"], ReversalCredited) and by_account["a"].amount == 40
    )
    assert all(e.transfer_id == revert_id and e.reverts == original_id for e in mirror)
    restored = _apply_all(after, mirror)
    assert (restored["a"].balance, restored["b"].balance) == (100, 0)


def test_n_leg_set_mirrors_every_leg_exactly() -> None:
    original_id = uuid4()
    command = Post(
        "payer",
        (Leg("payer", 100, "debit"), Leg("m", 97, "credit"), Leg("fees", 3, "credit")),
        1,
    )
    states = _states(payer=100, m=0, fees=0)
    original = decide_postings(states, command, transfer_id=original_id)
    after = _apply_all(states, original)
    mirror = decide_revert(
        after,
        original,
        Revert("payer", original_id, 2),
        transfer_id=uuid4(),
        reverted_by=None,
    )
    amounts = {(type(e).__name__, e.account_id): e.amount for e in mirror}
    assert amounts == {
        ("ReversalCredited", "payer"): 100,
        ("ReversalDebited", "m"): 97,
        ("ReversalDebited", "fees"): 3,
    }
    restored = _apply_all(after, mirror)
    assert {n: s.balance for n, s in restored.items()} == {
        "payer": 100,
        "m": 0,
        "fees": 0,
    }


def test_posted_hold_is_revertible_but_its_lifecycle_events_are_ignored() -> None:
    hold_id = uuid4()
    later = "2099-01-01T00:00:00+00:00"
    legs = [
        HoldPlaced("src", 40, hold_id, "dst", later),  # lifecycle: not a movement
        HoldPosted("src", 30, hold_id, "dst"),
        TransferCredited("dst", 30, hold_id, "src"),
        HoldReleased("src", 10, hold_id, "partial"),  # lifecycle: not a movement
    ]
    states = {"src": AccountState("src", 70, 5), "dst": AccountState("dst", 30, 1)}
    mirror = decide_revert(
        states, legs, Revert("src", hold_id, 5), transfer_id=uuid4(), reverted_by=None
    )
    assert [(type(e).__name__, e.account_id, e.amount) for e in mirror] == [
        ("ReversalCredited", "src", 30),
        ("ReversalDebited", "dst", 30),
    ]
    assert all(
        isinstance(leg, MOVEMENT_LEGS) is (i in (1, 2)) for i, leg in enumerate(legs)
    )


def test_revert_of_a_revert_restores_the_original_effect() -> None:
    t, r1, r2 = uuid4(), uuid4(), uuid4()
    s0 = _states(a=100, b=0)
    original = decide_postings(s0, Transfer("a", "b", 40, 1), transfer_id=t)
    s1 = _apply_all(s0, original)
    first = decide_revert(
        s1, original, Revert("a", t, 2), transfer_id=r1, reverted_by=None
    )
    s2 = _apply_all(s1, first)
    assert (s2["a"].balance, s2["b"].balance) == (100, 0)
    # The second revert mirrors the FIRST revert: b is credited again.
    second = decide_revert(
        s2, first, Revert("b", r1, 2), transfer_id=r2, reverted_by=None
    )
    s3 = _apply_all(s2, second)
    assert (s3["a"].balance, s3["b"].balance) == (60, 40)
    assert all(e.reverts == r1 for e in second)


# -- rejections -----------------------------------------------------------------------


def test_cash_movements_and_hold_lifecycle_alone_are_not_revertible() -> None:
    with pytest.raises(NotRevertibleError):
        decide_revert(
            _states(a=100),
            [Deposited("a", 100)],
            Revert("a", uuid4(), 1),
            transfer_id=uuid4(),
            reverted_by=None,
        )
    with pytest.raises(NotRevertibleError):
        decide_revert(
            _states(a=100),
            [],
            Revert("a", uuid4(), 1),
            transfer_id=uuid4(),
            reverted_by=None,
        )
    hold_id = uuid4()
    with pytest.raises(NotRevertibleError):
        decide_revert(
            {"a": AccountState("a", 100, 2, held=40)},
            [HoldPlaced("a", 40, hold_id, "b", "2099-01-01T00:00:00+00:00")],
            Revert("a", hold_id, 2),
            transfer_id=uuid4(),
            reverted_by=None,
        )


def test_second_revert_is_already_reverted() -> None:
    t = uuid4()
    original = decide_postings(
        _states(a=100, b=0), Transfer("a", "b", 40, 1), transfer_id=t
    )
    with pytest.raises(AlreadyRevertedError):
        decide_revert(
            _states(a=60, b=40),
            original,
            Revert("a", t, 1),
            transfer_id=uuid4(),
            reverted_by=uuid4(),
        )


def test_anchor_must_be_an_account_the_revert_credits() -> None:
    t = uuid4()
    original = decide_postings(
        _states(a=100, b=0), Transfer("a", "b", 40, 1), transfer_id=t
    )
    with pytest.raises(
        AnchorNotCreditedError
    ):  # b received the money; reverting debits b
        decide_revert(
            _states(a=60, b=40),
            original,
            Revert("b", t, 1),
            transfer_id=uuid4(),
            reverted_by=None,
        )


def test_payee_who_spent_the_money_cannot_be_reverted_into_debt() -> None:
    t = uuid4()
    original = decide_postings(
        _states(a=100, b=0), Transfer("a", "b", 40, 1), transfer_id=t
    )
    spent = {"a": AccountState("a", 60, 2), "b": AccountState("b", 39, 2)}
    with pytest.raises(InsufficientFundsError):
        decide_revert(
            spent, original, Revert("a", t, 2), transfer_id=uuid4(), reverted_by=None
        )
    # Held funds are not available either.
    held = {"a": AccountState("a", 60, 2), "b": AccountState("b", 40, 2, held=1)}
    with pytest.raises(InsufficientFundsError):
        decide_revert(
            held, original, Revert("a", t, 2), transfer_id=uuid4(), reverted_by=None
        )


def test_unknown_command_is_refused() -> None:
    with pytest.raises(UnknownCommandError):
        decide_revert({}, [], object(), transfer_id=uuid4(), reverted_by=None)  # type: ignore[arg-type]


def test_reversal_events_validate_and_round_trip() -> None:
    with pytest.raises(ValueError, match="revert itself"):
        same = uuid4()
        ReversalDebited("a", 1, same, "b", same)
    event = ReversalCredited("a", 40, uuid4(), "b", uuid4())
    from datetime import UTC, datetime

    envelope = EventEnvelope.from_domain_event(
        event,
        event_id=uuid4(),
        stream_version=1,
        occurred_at=datetime.now(UTC),
        correlation_id=uuid4(),
        causation_id=uuid4(),
        command_id=uuid4(),
    )
    assert EventEnvelope.from_dict(envelope.to_dict()).to_domain_event() == event


# -- conservation with reverts ----------------------------------------------------------


@st.composite
def transfer_then_revert_sequences(draw):
    deposit = draw(st.integers(min_value=10, max_value=1_000))
    steps = draw(
        st.lists(
            st.sampled_from(["transfer", "revert_last", "revert_revert"]),
            min_size=1,
            max_size=10,
        )
    )
    return deposit, steps


@settings(max_examples=150, deadline=None)
@given(transfer_then_revert_sequences())
def test_reverts_conserve_money_and_never_go_negative(case) -> None:
    deposit, steps = case
    states = {"a": fold([Deposited("a", deposit)]), "b": AccountState()}
    committed: list[tuple[object, tuple]] = []  # (transfer_id, legs) in order
    reverted: set = set()
    for step in steps:
        if step == "transfer":
            amount = max(1, min(states["a"].available, 7))
            if states["a"].available < 1:
                continue
            tid = uuid4()
            legs = decide_postings(
                states, Transfer("a", "b", amount, states["a"].version), transfer_id=tid
            )
            states = _apply_all(states, legs)
            committed.append((tid, legs))
        elif committed:
            candidates = [(tid, legs) for tid, legs in committed if tid not in reverted]
            if not candidates:
                continue
            tid, legs = candidates[-1] if step == "revert_last" else candidates[0]
            anchor = next(
                e.account_id for e in legs if type(e).__name__.endswith("Debited")
            )
            try:
                mirror = decide_revert(
                    states,
                    legs,
                    Revert(anchor, tid, states[anchor].version),
                    transfer_id=uuid4(),
                    reverted_by=None,
                )
            except InsufficientFundsError:
                continue
            states = _apply_all(states, mirror)
            reverted.add(tid)
            committed.append((mirror[0].transfer_id, mirror))
        assert states["a"].balance + states["b"].balance == deposit
        assert all(s.balance >= 0 for s in states.values())
