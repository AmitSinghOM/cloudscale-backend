"""ADR-0014: holds -- reserve, post (full or partial), void, expire.

The fold never reads a clock; only the post/expire *decisions* do, once,
through the ``now`` argument. Conservation extends to held funds: a hold
moves nothing, a post moves exactly what a transfer would, a release
moves nothing.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cloudscale.domain.account import (
    AccountState,
    OpenHold,
    apply,
    decide,
    decide_hold,
    decide_post_hold,
    decide_release_hold,
    fold,
    open_hold_from_events,
)
from cloudscale.domain.commands import ExpireHold, Hold, PostHold, VoidHold, Withdraw
from cloudscale.domain.errors import (
    CaptureExceedsHoldError,
    HoldExpiredError,
    HoldNotExpiredError,
    HoldNotOpenError,
    InsufficientFundsError,
    InvalidAccountStateError,
    InvalidExpiryError,
    SameAccountError,
)
from cloudscale.domain.events import (
    Deposited,
    EventEnvelope,
    HoldPlaced,
    HoldPosted,
    HoldReleased,
    TransferCredited,
)

NOW = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
LATER = (NOW + timedelta(hours=1)).isoformat()
EARLIER = (NOW - timedelta(hours=1)).isoformat()


def _src(balance: int, held: int = 0, version: int = 1) -> AccountState:
    return AccountState("src", balance, version, held)


def _open(amount: int, expires_at: str = LATER, hold_id=None) -> OpenHold:
    return OpenHold(hold_id or uuid4(), amount, "dst", expires_at)


# -- state ------------------------------------------------------------------------------


def test_available_is_balance_minus_held_and_held_cannot_exceed_balance() -> None:
    assert _src(100, held=30).available == 70
    with pytest.raises(InvalidAccountStateError):
        AccountState("src", 10, 1, held=11)
    with pytest.raises(InvalidAccountStateError):
        AccountState("src", 10, 1, held=-1)


def test_fold_tracks_held_through_place_post_and_release() -> None:
    hid = uuid4()
    events = [
        Deposited("src", 100),
        HoldPlaced("src", 40, hid, "dst", LATER),
        HoldPosted("src", 30, hid, "dst"),
        HoldReleased("src", 10, hid, "partial"),
    ]
    states = [fold(events[:k]) for k in range(1, 5)]
    assert (states[0].balance, states[0].held) == (100, 0)
    assert (states[1].balance, states[1].held, states[1].available) == (100, 40, 60)
    assert (states[2].balance, states[2].held) == (70, 10)
    assert (states[3].balance, states[3].held, states[3].available) == (70, 0, 70)


def test_release_beyond_held_is_an_invariant_violation_not_a_negative_balance() -> None:
    with pytest.raises(InvalidAccountStateError, match="held funds negative"):
        apply(_src(100, held=5), HoldReleased("src", 6, uuid4(), "voided"))


# -- Hold ---------------------------------------------------------------------------------


def test_hold_is_checked_against_available_not_balance() -> None:
    state = _src(100, held=60)
    with pytest.raises(InsufficientFundsError):
        decide_hold(state, Hold("src", "dst", 41, 1, 3600), hold_id=uuid4(), now=NOW)
    placed = decide_hold(
        state, Hold("src", "dst", 40, 1, 3600), hold_id=uuid4(), now=NOW
    )
    assert placed.amount == 40 and placed.counterparty == "dst"
    assert placed.expires_at == LATER  # stamped from the decision clock + ttl
    assert apply(state, placed).available == 0


def test_every_debit_honours_available_funds() -> None:
    state = _src(100, held=60)
    with pytest.raises(InsufficientFundsError):
        decide(state, Withdraw("src", 41, 1))
    assert decide(state, Withdraw("src", 40, 1)).amount == 40


@pytest.mark.parametrize("bad", [0, -1, True, "3600", 1.5])
def test_hold_ttl_must_be_a_positive_integer(bad) -> None:
    with pytest.raises(InvalidExpiryError):
        Hold("src", "dst", 1, 0, bad)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "bad", ["", "not-a-date", "2026-09-20T12:00:00", "2026-09-20T12:00:00+05:30"]
)
def test_hold_placed_event_expiry_must_be_utc_iso8601(bad) -> None:
    with pytest.raises(ValueError):
        HoldPlaced("src", 1, uuid4(), "dst", bad)


def test_hold_rejects_same_account() -> None:
    with pytest.raises(SameAccountError):
        Hold("src", "src", 1, 0, 3600)


# -- open-hold derivation -------------------------------------------------------------------


def test_open_hold_derivation_from_the_streams_events() -> None:
    hid, other = uuid4(), uuid4()
    placed = HoldPlaced("src", 40, hid, "dst", LATER)
    assert open_hold_from_events(hid, [placed]) == OpenHold(hid, 40, "dst", LATER)
    assert open_hold_from_events(hid, []) is None
    assert (
        open_hold_from_events(hid, [HoldPlaced("src", 5, other, "dst", LATER)]) is None
    )
    assert (
        open_hold_from_events(hid, [placed, HoldPosted("src", 40, hid, "dst")]) is None
    )
    assert (
        open_hold_from_events(hid, [placed, HoldReleased("src", 40, hid, "voided")])
        is None
    )
    assert (
        open_hold_from_events(hid, [placed, HoldReleased("src", 40, hid, "expired")])
        is None
    )
    # A partial release alone does not close a hold (it always accompanies a post).
    assert (
        open_hold_from_events(hid, [placed, HoldReleased("src", 10, hid, "partial")])
        is not None
    )


# -- PostHold -------------------------------------------------------------------------------


def test_post_full_capture_yields_debit_and_credit_sharing_the_hold_id() -> None:
    hold = _open(40)
    events = decide_post_hold(
        _src(100, held=40),
        AccountState("dst", 0, 0),
        hold,
        PostHold("src", hold.hold_id, 1),
        now=NOW,
    )
    assert [type(e).__name__ for e in events] == ["HoldPosted", "TransferCredited"]
    posted, credited = events
    assert isinstance(posted, HoldPosted) and isinstance(credited, TransferCredited)
    assert posted.amount == credited.amount == 40
    assert credited.transfer_id == hold.hold_id and credited.counterparty == "src"


def test_post_partial_capture_releases_the_remainder_in_the_same_decision() -> None:
    hold = _open(40)
    events = decide_post_hold(
        _src(100, held=40),
        AccountState("dst", 0, 0),
        hold,
        PostHold("src", hold.hold_id, 1, amount=15),
        now=NOW,
    )
    assert [type(e).__name__ for e in events] == [
        "HoldPosted",
        "TransferCredited",
        "HoldReleased",
    ]
    assert events[2].amount == 25 and events[2].reason == "partial"  # type: ignore[union-attr]
    src = _src(100, held=40)
    for event in events:
        if event.account_id == "src":
            src = apply(src, event)
    assert (src.balance, src.held, src.available) == (85, 0, 85)


@pytest.mark.parametrize(
    "hold, command_amount, error",
    [
        (None, None, HoldNotOpenError),
        (_open(40, expires_at=EARLIER), None, HoldExpiredError),
        (_open(40), 41, CaptureExceedsHoldError),
    ],
)
def test_post_rejections(hold, command_amount, error) -> None:
    hold_id = hold.hold_id if hold else uuid4()
    with pytest.raises(error):
        decide_post_hold(
            _src(100, held=40),
            AccountState("dst", 0, 0),
            hold,
            PostHold("src", hold_id, 1, command_amount),
            now=NOW,
        )


def test_post_exactly_at_expiry_is_expired() -> None:
    hold = _open(40, expires_at=NOW.isoformat())
    with pytest.raises(HoldExpiredError):
        decide_post_hold(
            _src(100, held=40),
            AccountState("dst", 0, 0),
            hold,
            PostHold("src", hold.hold_id, 1),
            now=NOW,
        )


# -- Void / Expire ------------------------------------------------------------------------


def test_void_releases_any_time_while_open() -> None:
    hold = _open(40, expires_at=EARLIER)  # even past expiry
    released = decide_release_hold(
        _src(100, held=40), hold, VoidHold("src", hold.hold_id, 1), now=NOW
    )
    assert (released.amount, released.reason) == (40, "voided")
    assert apply(_src(100, held=40), released).available == 100


def test_expire_requires_expiry_to_have_passed() -> None:
    hold = _open(40)
    with pytest.raises(HoldNotExpiredError):
        decide_release_hold(
            _src(100, held=40), hold, ExpireHold("src", hold.hold_id, 1), now=NOW
        )
    expired = decide_release_hold(
        _src(100, held=40),
        _open(40, expires_at=EARLIER, hold_id=hold.hold_id),
        ExpireHold("src", hold.hold_id, 1),
        now=NOW,
    )
    assert expired.reason == "expired"


def test_release_of_a_closed_hold_is_hold_not_open() -> None:
    with pytest.raises(HoldNotOpenError):
        decide_release_hold(_src(100), None, VoidHold("src", uuid4(), 1), now=NOW)


# -- envelope round trip ------------------------------------------------------------------


@pytest.mark.parametrize(
    "event",
    [
        HoldPlaced("src", 40, uuid4(), "dst", LATER),
        HoldReleased("src", 40, uuid4(), "expired"),
        HoldPosted("src", 40, uuid4(), "dst"),
    ],
)
def test_hold_events_round_trip_through_the_envelope(event) -> None:
    envelope = EventEnvelope.from_domain_event(
        event,
        event_id=uuid4(),
        stream_version=1,
        occurred_at=NOW,
        correlation_id=uuid4(),
        causation_id=uuid4(),
        command_id=uuid4(),
    )
    assert envelope.schema_version == 1
    assert EventEnvelope.from_dict(envelope.to_dict()).to_domain_event() == event


# -- conservation -------------------------------------------------------------------------


@st.composite
def hold_lifecycles(draw):
    """A deposit, then a sequence of place / post(full|partial) / void / expire."""
    deposit = draw(st.integers(min_value=1, max_value=1_000))
    steps = draw(
        st.lists(
            st.sampled_from(["place", "post", "partial", "void", "expire"]), max_size=12
        )
    )
    return deposit, steps


def _apply_step(
    step: str, src: AccountState, dst: AccountState, open_holds: list[OpenHold]
):
    """Apply one lifecycle step; returns the new (src, dst)."""
    if step == "place":
        amount = min(src.available, 7) or 1
        try:
            placed = decide_hold(
                src,
                Hold("src", "dst", amount, src.version, 3600),
                hold_id=uuid4(),
                now=NOW,
            )
        except InsufficientFundsError:
            return src, dst
        open_holds.append(OpenHold(placed.hold_id, placed.amount, "dst", LATER))
        return apply(src, placed), dst
    if not open_holds:
        return src, dst
    hold = open_holds.pop(0)
    if step in ("post", "partial"):
        amount = None if step == "post" else max(1, hold.amount // 2)
        command = PostHold("src", hold.hold_id, src.version, amount)
        for event in decide_post_hold(src, dst, hold, command, now=NOW):
            if event.account_id == "src":
                src = apply(src, event)
            else:
                dst = apply(dst, event)
        return src, dst
    released = decide_release_hold(
        src, hold, VoidHold("src", hold.hold_id, src.version), now=NOW
    )
    return apply(src, released), dst


@settings(max_examples=150, deadline=None)
@given(hold_lifecycles())
def test_holds_conserve_money_and_keep_held_within_balance(case) -> None:
    deposit, steps = case
    src = fold([Deposited("src", deposit)])
    dst = AccountState()
    open_holds: list[OpenHold] = []
    for step in steps:
        src, dst = _apply_step(step, src, dst, open_holds)
        # Invariants at every step.
        assert src.balance + dst.balance == deposit
        assert 0 <= src.held <= src.balance
        assert src.held == sum(h.amount for h in open_holds)
