"""ADR-0013: N-leg balanced postings.

Conservation is a property of the *command*: an accepted ``Post`` cannot move
money into or out of the system. Everything the command refuses is refused
before any state is consulted.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cloudscale.domain.account import AccountState, apply, decide_postings, fold
from cloudscale.domain.commands import MAX_LEGS, Leg, Post, Transfer
from cloudscale.domain.errors import (
    AmountOutOfRangeError,
    AnchorNotDebitedError,
    DuplicateAccountError,
    InsufficientFundsError,
    InvalidAmountError,
    TooManyLegsError,
    UnbalancedPostingError,
    UnknownCommandError,
)
from cloudscale.domain.events import TransferCredited, TransferDebited
from cloudscale.domain.commands import MAX_SIGNED_BIGINT


def _states(**balances: int) -> dict[str, AccountState]:
    return {name: AccountState(name, balance, 1) for name, balance in balances.items()}


# -- command validation -------------------------------------------------------------


def test_balanced_three_way_post_is_accepted_and_names_the_anchor_as_counterparty():
    command = Post(
        "payer",
        (
            Leg("payer", 100, "debit"),
            Leg("merchant", 97, "credit"),
            Leg("fees", 3, "credit"),
        ),
        expected_version=1,
    )
    tid = uuid4()
    events = decide_postings(
        _states(payer=100, merchant=0, fees=0), command, transfer_id=tid
    )
    assert [type(e).__name__ for e in events] == [
        "TransferDebited",
        "TransferCredited",
        "TransferCredited",
    ]
    assert all(e.transfer_id == tid for e in events)
    by_account = {e.account_id: e for e in events}
    # N > 2: every other leg points at the payer; the payer's own leg names
    # its largest payee (an event may not name itself as counterparty).
    assert by_account["merchant"].counterparty == "payer"
    assert by_account["fees"].counterparty == "payer"
    assert by_account["payer"].counterparty == "merchant"


def test_two_leg_post_uses_the_other_account_as_counterparty():
    command = Post("a", (Leg("a", 5, "debit"), Leg("b", 5, "credit")), 1)
    debit, credit = decide_postings(_states(a=5, b=0), command, transfer_id=uuid4())
    assert debit.counterparty == "b" and credit.counterparty == "a"


def test_transfer_and_equivalent_post_decide_the_same_events():
    tid = uuid4()
    states = _states(a=50, b=10)
    via_transfer = decide_postings(states, Transfer("a", "b", 20, 1), transfer_id=tid)
    via_post = decide_postings(
        states,
        Post("a", (Leg("a", 20, "debit"), Leg("b", 20, "credit")), 1),
        transfer_id=tid,
    )
    assert via_transfer == via_post


@pytest.mark.parametrize(
    "legs, error",
    [
        ((Leg("a", 10, "debit"), Leg("b", 9, "credit")), UnbalancedPostingError),
        ((Leg("a", 10, "debit"),), UnbalancedPostingError),
        ((Leg("a", 10, "debit"), Leg("a", 10, "credit")), DuplicateAccountError),
        ((Leg("b", 10, "debit"), Leg("a", 10, "credit")), AnchorNotDebitedError),
        ((Leg("a", 10, "credit"), Leg("b", 10, "debit")), AnchorNotDebitedError),
    ],
)
def test_invalid_posting_sets_are_refused_at_construction(legs, error):
    with pytest.raises(error):
        Post("a", legs, 1)


def test_too_many_legs_is_refused():
    legs = [Leg("a", MAX_LEGS, "debit")] + [
        Leg(f"c{i}", 1, "credit") for i in range(MAX_LEGS)
    ]  # MAX_LEGS + 1 legs, balanced
    with pytest.raises(TooManyLegsError):
        Post("a", tuple(legs), 1)
    # Exactly MAX_LEGS is fine.
    ok = [Leg("a", MAX_LEGS - 1, "debit")] + [
        Leg(f"c{i}", 1, "credit") for i in range(MAX_LEGS - 1)
    ]
    assert len(Post("a", tuple(ok), 1).postings) == MAX_LEGS


def test_leg_validation_reuses_the_amount_and_direction_rules():
    with pytest.raises(InvalidAmountError):
        Leg("a", 0, "debit")
    with pytest.raises(InvalidAmountError):
        Leg("a", 1, "sideways")  # type: ignore[arg-type]
    with pytest.raises(AmountOutOfRangeError):
        Leg("a", MAX_SIGNED_BIGINT + 1, "debit")


def test_list_of_legs_is_accepted_and_frozen_as_a_tuple():
    command = Post("a", [Leg("a", 1, "debit"), Leg("b", 1, "credit")], 0)  # type: ignore[arg-type]
    assert isinstance(command.postings, tuple)


# -- decision rules -------------------------------------------------------------------


def test_one_underfunded_debit_rejects_the_whole_set():
    command = Post(
        "a",
        (Leg("a", 10, "debit"), Leg("b", 10, "debit"), Leg("c", 20, "credit")),
        1,
    )
    with pytest.raises(InsufficientFundsError):
        decide_postings(_states(a=10, b=9, c=0), command, transfer_id=uuid4())


def test_one_overflowing_credit_rejects_the_whole_set():
    command = Post("a", (Leg("a", 10, "debit"), Leg("b", 10, "credit")), 1)
    with pytest.raises(AmountOutOfRangeError):
        decide_postings(
            _states(a=10, b=MAX_SIGNED_BIGINT - 5), command, transfer_id=uuid4()
        )


def test_unknown_command_is_refused():
    with pytest.raises(UnknownCommandError):
        decide_postings({}, object(), transfer_id=uuid4())  # type: ignore[arg-type]


# -- conservation ---------------------------------------------------------------------


@st.composite
def balanced_posting_sets(draw):
    n_accounts = draw(st.integers(min_value=2, max_value=6))
    names = [f"acct-{i}" for i in range(n_accounts)]
    balances = {
        name: draw(st.integers(min_value=0, max_value=10_000)) for name in names
    }
    # Anchor debits some amount it can afford; split it across other accounts as credits,
    # optionally adding a second debited account so the set has debits on both sides.
    anchor = names[0]
    balances[anchor] = max(balances[anchor], 1)
    debit_amount = draw(st.integers(min_value=1, max_value=balances[anchor]))
    others = names[1:]
    second_debtor = draw(st.sampled_from([None, *others]))
    legs = [Leg(anchor, debit_amount, "debit")]
    total = debit_amount
    if second_debtor is not None and balances[second_debtor] > 0:
        extra = draw(st.integers(min_value=1, max_value=balances[second_debtor]))
        legs.append(Leg(second_debtor, extra, "debit"))
        total += extra
    creditors = [n for n in others if n != second_debtor]
    if not creditors:
        creditors = [others[0]]
        legs = [leg for leg in legs if leg.account_id != others[0]]
        total = sum(leg.amount for leg in legs)
    # Split total across creditors (each >= 1 where possible).
    cuts = sorted(
        draw(
            st.lists(
                st.integers(min_value=0, max_value=total), max_size=len(creditors) - 1
            )
        )
    )
    shares, prev = [], 0
    for cut in cuts + [total]:
        shares.append(cut - prev)
        prev = cut
    for name, share in zip(creditors, shares):
        if share > 0:
            legs.append(Leg(name, share, "credit"))
    return balances, Post(anchor, tuple(legs), 1)


@settings(max_examples=200, deadline=None)
@given(balanced_posting_sets())
def test_accepted_posting_sets_conserve_money_and_never_go_negative(case):
    balances, command = case
    states = {name: AccountState(name, bal, 1) for name, bal in balances.items()}
    events = decide_postings(states, command, transfer_id=uuid4())
    after = dict(states)
    for event in events:
        after[event.account_id] = apply(after[event.account_id], event)
    assert sum(s.balance for s in after.values()) == sum(
        s.balance for s in states.values()
    )
    assert all(s.balance >= 0 for s in after.values())
    assert len(events) == len(command.postings)
    assert {e.account_id for e in events} == {
        leg.account_id for leg in command.postings
    }


def test_fold_of_all_legs_matches_apply_sequence():
    a, b, c = (AccountState(n, 30, 1) for n in "abc")
    command = Post(
        "a", (Leg("a", 30, "debit"), Leg("b", 10, "credit"), Leg("c", 20, "credit")), 1
    )
    events = decide_postings({"a": a, "b": b, "c": c}, command, transfer_id=uuid4())
    by_account = {e.account_id: e for e in events}
    assert fold([by_account["a"]], a).balance == 0
    assert fold([by_account["b"]], b).balance == 40
    assert fold([by_account["c"]], c).balance == 50
    assert isinstance(by_account["a"], TransferDebited)
    assert isinstance(by_account["b"], TransferCredited)
