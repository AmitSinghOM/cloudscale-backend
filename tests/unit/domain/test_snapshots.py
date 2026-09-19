"""ADR-0012: a snapshot is a verified cache; folding from it equals the full fold.

The contract that makes snapshots safe is one equation checked at every cut
point of every stream: ``fold_from(snapshot_at(k), events[k:]) == fold(events)``.
Everything else here is the set of reasons a stored row must be discarded.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cloudscale.application import snapshots as snap
from cloudscale.application.snapshots import (
    SnapshotTracker,
    StreamSnapshot,
    fold_from_snapshot,
    snapshot_rejection,
)
from cloudscale.domain.account import CURRENT_STATE_VERSION, AccountState, apply, fold
from cloudscale.domain.events import (
    AccountEvent,
    Deposited,
    EventEnvelope,
    TransferCredited,
    TransferDebited,
    Withdrawn,
)

ACCOUNT = "acct-snap"


def _events(deltas: list[int]) -> list[AccountEvent]:
    """Turn signed deltas into a valid event stream (never negative)."""
    out: list[AccountEvent] = []
    balance = 0
    for i, delta in enumerate(deltas):
        amount = abs(delta) or 1
        if delta >= 0:
            kind = i % 2  # alternate the two credit shapes
            out.append(
                Deposited(ACCOUNT, amount)
                if kind == 0
                else TransferCredited(ACCOUNT, amount, uuid4(), "acct-other")
            )
            balance += amount
        else:
            amount = min(amount, balance)
            if amount == 0:
                out.append(Deposited(ACCOUNT, 1))
                balance += 1
                continue
            out.append(
                Withdrawn(ACCOUNT, amount)
                if i % 2 == 0
                else TransferDebited(ACCOUNT, amount, uuid4(), "acct-other")
            )
            balance -= amount
    return out


def _snapshot_at(events: list[AccountEvent], k: int) -> StreamSnapshot:
    """The snapshot a writer would have taken after event k (1-based seq)."""
    return StreamSnapshot(
        seq=k,
        state=fold(events[:k]),
        state_version=CURRENT_STATE_VERSION,
        anchor_event_id=f"evt-{k}",
    )


def _rows(events: list[AccountEvent], start_seq: int) -> list[dict]:
    return [
        {"seq": i + 1, "event_id": f"evt-{i + 1}", "type": type(e).__name__}
        for i, e in enumerate(events)
        if i + 1 >= start_seq
    ]


@settings(max_examples=150, deadline=None)
@given(st.lists(st.integers(min_value=-500, max_value=500), min_size=1, max_size=40))
def test_fold_from_snapshot_equals_full_fold_at_every_cut_point(deltas) -> None:
    events = _events(deltas)
    full = fold(events)
    for k in range(1, len(events) + 1):
        snapshot = _snapshot_at(events, k)
        assert snapshot_rejection(snapshot, _rows(events, k)) is None
        assert fold_from_snapshot(snapshot, events[k:]) == full


def test_fixture_corpus_streams_fold_identically_from_any_snapshot() -> None:
    events = _events([100, -30, 50, -20, 7, -7, 1000, -999])
    full = fold(events)
    for k in range(1, len(events) + 1):
        assert fold_from_snapshot(_snapshot_at(events, k), events[k:]) == full


# -- reasons a stored row is discarded ------------------------------------------------


def test_state_version_mismatch_is_rejected() -> None:
    events = _events([10, 20])
    stale = StreamSnapshot(
        seq=2,
        state=fold(events),
        state_version=CURRENT_STATE_VERSION + 1,
        anchor_event_id="evt-2",
    )
    reason = snapshot_rejection(stale, _rows(events, 2))
    assert reason is not None and "state_version" in reason


def test_anchor_past_end_of_stream_is_rejected() -> None:
    events = _events([10, 20])
    truncated_log_rows: list[dict] = []
    reason = snapshot_rejection(_snapshot_at(events, 2), truncated_log_rows)
    assert reason is not None and "past the end" in reason


def test_anchor_event_id_mismatch_is_rejected() -> None:
    events = _events([10, 20, 30])
    rows = _rows(events, 2)
    rows[0]["event_id"] = "evt-from-a-different-log"
    reason = snapshot_rejection(_snapshot_at(events, 2), rows)
    assert reason is not None and "anchor mismatch" in reason


def test_anchor_seq_mismatch_is_rejected() -> None:
    events = _events([10, 20, 30])
    rows = _rows(events, 3)  # log starts later than the snapshot claims
    reason = snapshot_rejection(_snapshot_at(events, 2), rows)
    assert reason is not None and "anchor mismatch" in reason


def test_internally_inconsistent_row_is_rejected() -> None:
    events = _events([10, 20, 30])
    broken = StreamSnapshot(
        seq=3,
        state=fold(events[:2]),
        state_version=CURRENT_STATE_VERSION,
        anchor_event_id="evt-3",
    )
    reason = snapshot_rejection(broken, _rows(events, 3))
    assert reason is not None and "state.version" in reason


def test_state_json_round_trips() -> None:
    state = AccountState(ACCOUNT, 12345, 7)
    text = StreamSnapshot(7, state, CURRENT_STATE_VERSION, "x").to_state_json()
    assert StreamSnapshot.state_from_json(text) == state


# -- when a writer takes a snapshot -----------------------------------------------------


def _envelope(event: AccountEvent, version: int) -> EventEnvelope:
    from datetime import UTC, datetime

    return EventEnvelope.from_domain_event(
        event,
        event_id=uuid4(),
        stream_version=version,
        occurred_at=datetime.now(UTC),
        correlation_id=uuid4(),
        causation_id=uuid4(),
        command_id=uuid4(),
    )


def test_tracker_snapshots_exactly_every_n_events_with_the_applied_state() -> None:
    tracker = SnapshotTracker(every=3)
    state = AccountState()
    tracker.folded(ACCOUNT, state, snapshot_seq=0)
    taken: list[StreamSnapshot] = []
    for version in range(1, 8):
        event = Deposited(ACCOUNT, 10)
        due = tracker.after_append(_envelope(event, version), event)
        state = apply(state, event)
        if due is not None:
            taken.append(due)
            assert due.state == state and due.seq == version
    assert [s.seq for s in taken] == [3, 6]
    assert all(s.state_version == CURRENT_STATE_VERSION for s in taken)


def test_tracker_counts_from_the_snapshot_it_folded_from() -> None:
    tracker = SnapshotTracker(every=5)
    tracker.folded(ACCOUNT, AccountState(ACCOUNT, 0, 7), snapshot_seq=5)
    event = Deposited(ACCOUNT, 1)
    assert tracker.after_append(_envelope(event, 8), event) is None
    assert tracker.after_append(_envelope(event, 9), event) is None
    assert tracker.after_append(_envelope(event, 10), event) is not None


@pytest.mark.parametrize("every", [0, -1])
def test_tracker_disabled_never_snapshots(every) -> None:
    tracker = SnapshotTracker(every=every)
    tracker.folded(ACCOUNT, AccountState(), 0)
    event = Deposited(ACCOUNT, 1)
    assert all(
        tracker.after_append(_envelope(event, v), event) is None for v in range(1, 50)
    )


def test_default_interval_is_documented_value() -> None:
    assert snap.DEFAULT_SNAPSHOT_EVERY == 100
