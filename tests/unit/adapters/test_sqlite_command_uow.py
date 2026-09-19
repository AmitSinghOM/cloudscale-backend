"""Contract tests for SqliteCommandUnitOfWork against the port's guarantees.

Replay identity, conflict semantics, rejection persistence without append,
transactional atomicity, restart durability, consumer visibility, and
serialized concurrent writers.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from cloudscale.adapters.sqlite_compat.command_unit_of_work import (
    SqliteCommandUnitOfWork,
)
from cloudscale.application.command_service import normalize_command
from cloudscale.application.ports import NormalizedCommand
from cloudscale.domain.account import CURRENT_STATE_VERSION
from cloudscale.domain.commands import (
    Deposit,
    ExpireHold,
    Hold,
    Leg,
    Post,
    PostHold,
    Revert,
    Transfer,
    VoidHold,
    Withdraw,
)
from cloudscale.domain.results import CommandOutcome
from cqrs import SqliteEventStore


def _request(
    command,
    *,
    command_id: UUID | None = None,
    issuer: str = "cloudscale",
    subject: str = "user-1",
) -> NormalizedCommand:
    return normalize_command(
        command,
        command_id=command_id or uuid4(),
        correlation_id=uuid4(),
        issuer=issuer,
        subject=subject,
    )


@pytest.fixture()
def db_path(tmp_path: Path) -> str:
    return str(tmp_path / "commands.db")


@pytest.fixture()
def uow(db_path: str) -> SqliteCommandUnitOfWork:
    unit = SqliteCommandUnitOfWork(db_path)
    yield unit
    unit.close()


def test_accepted_command_appends_to_the_consumer_visible_log(
    uow: SqliteCommandUnitOfWork, db_path: str
) -> None:
    result = uow.execute(_request(Deposit("acct-1", 100, 0)))

    assert result.outcome is CommandOutcome.ACCEPTED
    assert result.http_status == 201
    assert result.committed_version == 1
    assert result.event_id is not None

    # The event is in the SAME log the legacy store and consumer read.
    feed = SqliteEventStore(db_path)
    try:
        events = feed.read_all(0)
        assert len(events) == 1
        assert events[0]["event_id"] == str(result.event_id)
        assert events[0]["type"] == "Deposited"
        assert events[0]["amount"] == 100
        assert events[0]["stream"] == "account-acct-1"
    finally:
        feed.close()


def test_equal_hash_replay_returns_the_original_result_without_append(
    uow: SqliteCommandUnitOfWork, db_path: str
) -> None:
    command_id = uuid4()
    first = uow.execute(_request(Deposit("acct-1", 100, 0), command_id=command_id))
    # Same business request, new correlation id (trace-independent retry).
    replay = uow.execute(_request(Deposit("acct-1", 100, 0), command_id=command_id))

    assert replay == first  # byte-for-byte persisted identity
    feed = SqliteEventStore(db_path)
    try:
        assert len(feed.read_all(0)) == 1  # no second append
    finally:
        feed.close()


def test_reused_command_id_with_different_hash_conflicts_and_is_not_persisted(
    uow: SqliteCommandUnitOfWork,
) -> None:
    command_id = uuid4()
    original = uow.execute(_request(Deposit("acct-1", 100, 0), command_id=command_id))

    conflict = uow.execute(_request(Deposit("acct-1", 999, 0), command_id=command_id))
    assert conflict.outcome is CommandOutcome.COMMAND_ID_CONFLICT
    assert conflict.http_status == 409
    assert conflict.error_code == "command_id_conflict"

    # The original persisted record is untouched: equal-hash replay still works.
    replay = uow.execute(_request(Deposit("acct-1", 100, 0), command_id=command_id))
    assert replay == original


def test_version_conflict_is_persisted_without_append(
    uow: SqliteCommandUnitOfWork, db_path: str
) -> None:
    uow.execute(_request(Deposit("acct-1", 100, 0)))

    command_id = uuid4()
    stale = _request(Deposit("acct-1", 50, 0), command_id=command_id)  # stale: v1 now
    result = uow.execute(stale)
    assert result.outcome is CommandOutcome.VERSION_CONFLICT
    assert result.http_status == 409
    assert result.current_version == 1
    assert result.committed_version is None and result.event_id is None

    # Persisted: replay returns the identical stored rejection.
    assert (
        uow.execute(_request(Deposit("acct-1", 50, 0), command_id=command_id)) == result
    )
    feed = SqliteEventStore(db_path)
    try:
        assert len(feed.read_all(0)) == 1
    finally:
        feed.close()


def test_insufficient_funds_is_persisted_without_append(
    uow: SqliteCommandUnitOfWork, db_path: str
) -> None:
    uow.execute(_request(Deposit("acct-1", 30, 0)))
    command_id = uuid4()
    result = uow.execute(_request(Withdraw("acct-1", 50, 1), command_id=command_id))

    assert result.outcome is CommandOutcome.INSUFFICIENT_FUNDS
    assert result.http_status == 422
    assert result.error_code == "insufficient_funds"
    assert (
        uow.execute(_request(Withdraw("acct-1", 50, 1), command_id=command_id))
        == result
    )
    feed = SqliteEventStore(db_path)
    try:
        assert len(feed.read_all(0)) == 1
    finally:
        feed.close()


def test_domain_rejection_maps_to_400(uow: SqliteCommandUnitOfWork) -> None:
    max_bigint = 2**63 - 1
    uow.execute(_request(Deposit("acct-1", max_bigint - 5, 0)))
    result = uow.execute(_request(Deposit("acct-1", 10, 1)))

    assert result.outcome is CommandOutcome.DOMAIN_REJECTED
    assert result.http_status == 400
    assert result.error_code == "amount_out_of_range"


def test_failed_append_rolls_back_everything(db_path: str) -> None:
    """A mid-transaction failure must persist NOTHING (no result, no event)."""
    fixed_event_id = uuid4()
    uow = SqliteCommandUnitOfWork(db_path, event_id_factory=lambda: fixed_event_id)
    try:
        uow.execute(_request(Deposit("acct-1", 10, 0)))
        # Second accepted command reuses the same event_id -> UNIQUE(event_id)
        # violation inside the transaction, after decide succeeded.
        doomed_id = uuid4()
        with pytest.raises(sqlite3.IntegrityError):
            uow.execute(_request(Deposit("acct-1", 20, 1), command_id=doomed_id))

        # No result row leaked: an equal replay re-runs the decision (and hits
        # the same failure) instead of returning a stored result.
        with pytest.raises(sqlite3.IntegrityError):
            uow.execute(_request(Deposit("acct-1", 20, 1), command_id=doomed_id))
    finally:
        uow.close()

    feed = SqliteEventStore(db_path)
    try:
        assert len(feed.read_all(0)) == 1  # only the first accepted event
    finally:
        feed.close()


def test_results_survive_restart(db_path: str) -> None:
    command_id = uuid4()
    first = SqliteCommandUnitOfWork(db_path)
    original = first.execute(_request(Deposit("acct-1", 100, 0), command_id=command_id))
    first.close()

    second = SqliteCommandUnitOfWork(db_path)
    try:
        replay = second.execute(
            _request(Deposit("acct-1", 100, 0), command_id=command_id)
        )
        assert replay == original
    finally:
        second.close()


def test_concurrent_same_version_writers_yield_one_accept_one_conflict(
    uow: SqliteCommandUnitOfWork,
) -> None:
    uow.execute(_request(Deposit("acct-race", 100, 0)))
    barrier = threading.Barrier(2)
    outcomes: list[CommandOutcome] = []
    lock = threading.Lock()

    def contend() -> None:
        request = _request(Withdraw("acct-race", 60, 1))
        barrier.wait()
        result = uow.execute(request)
        with lock:
            outcomes.append(result.outcome)

    threads = [threading.Thread(target=contend) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(outcome.value for outcome in outcomes) == [
        "accepted",
        "version_conflict",
    ]


# -- double-entry transfers (ADR-0011) ------------------------------------------------


def test_transfer_appends_both_legs_atomically_and_reports_two_postings(
    uow: SqliteCommandUnitOfWork, db_path: str
) -> None:
    uow.execute(_request(Deposit("src", 100, 0)))
    command_id = uuid4()
    result = uow.execute(_request(Transfer("src", "dst", 40, 1), command_id=command_id))

    assert result.outcome is CommandOutcome.ACCEPTED
    assert result.account_id == "src"
    assert result.committed_version == 2
    assert [p.account_id for p in result.postings] == ["dst", "src"]  # sorted order
    by_account = {p.account_id: p for p in result.postings}
    assert by_account["src"].committed_version == 2
    assert by_account["dst"].committed_version == 1
    assert by_account["src"].event_id == result.event_id

    feed = SqliteEventStore(db_path)
    try:
        legs = [e for e in feed.read_all(0) if e["type"].startswith("Transfer")]
        assert {e["type"] for e in legs} == {"TransferDebited", "TransferCredited"}
        assert {e["transfer_id"] for e in legs} == {str(command_id)}
        assert {e["counterparty"] for e in legs} == {"src", "dst"}
    finally:
        feed.close()

    assert uow.fold_stream("src").balance == 60
    assert uow.fold_stream("dst").balance == 40


def test_transfer_replay_and_conflict_follow_the_command_contract(
    uow: SqliteCommandUnitOfWork,
) -> None:
    uow.execute(_request(Deposit("src", 100, 0)))
    command_id = uuid4()
    first = uow.execute(_request(Transfer("src", "dst", 40, 1), command_id=command_id))
    replay = uow.execute(_request(Transfer("src", "dst", 40, 1), command_id=command_id))
    assert replay == first  # stored result, nothing re-executed
    assert uow.fold_stream("dst").balance == 40

    changed = uow.execute(
        _request(Transfer("src", "dst", 41, 1), command_id=command_id)
    )
    assert changed.outcome is CommandOutcome.COMMAND_ID_CONFLICT
    assert changed.postings == ()

    stale = uow.execute(_request(Transfer("src", "dst", 1, 1)))
    assert stale.outcome is CommandOutcome.VERSION_CONFLICT
    assert stale.current_version == 2
    assert uow.fold_stream("dst").version == 1  # nothing appended on either stream


def test_transfer_rejections_persist_without_touching_either_stream(
    uow: SqliteCommandUnitOfWork,
) -> None:
    uow.execute(_request(Deposit("src", 10, 0)))
    request = _request(Transfer("src", "dst", 11, 1))
    result = uow.execute(request)
    assert result.outcome is CommandOutcome.INSUFFICIENT_FUNDS
    assert result.http_status == 422
    assert uow.fold_stream("src").balance == 10
    assert uow.fold_stream("src").version == 1
    assert uow.fold_stream("dst").version == 0
    # Persisted: the same command_id replays the rejection.
    assert uow.execute(request) == result


# -- N-leg postings (ADR-0013) ---------------------------------------------------------


def test_post_appends_every_leg_atomically_in_account_order(
    uow: SqliteCommandUnitOfWork, db_path: str
) -> None:
    uow.execute(_request(Deposit("payer", 100, 0)))
    command_id = uuid4()
    command = Post(
        "payer",
        (
            Leg("payer", 100, "debit"),
            Leg("merchant", 97, "credit"),
            Leg("fees", 3, "credit"),
        ),
        1,
    )
    result = uow.execute(_request(command, command_id=command_id))

    assert result.outcome is CommandOutcome.ACCEPTED
    assert result.account_id == "payer" and result.committed_version == 2
    assert [p.account_id for p in result.postings] == ["fees", "merchant", "payer"]
    assert {p.committed_version for p in result.postings} == {1, 1, 2}

    feed = SqliteEventStore(db_path)
    try:
        legs = [e for e in feed.read_all(0) if e["type"].startswith("Transfer")]
        assert len(legs) == 3
        assert {e["transfer_id"] for e in legs} == {str(command_id)}
    finally:
        feed.close()

    assert uow.fold_stream("payer").balance == 0
    assert uow.fold_stream("merchant").balance == 97
    assert uow.fold_stream("fees").balance == 3


def test_post_with_one_underfunded_leg_appends_nothing_anywhere(
    uow: SqliteCommandUnitOfWork,
) -> None:
    uow.execute(_request(Deposit("a", 10, 0)))
    uow.execute(_request(Deposit("b", 5, 0)))
    request = _request(
        Post(
            "a",
            (Leg("a", 10, "debit"), Leg("b", 6, "debit"), Leg("c", 16, "credit")),
            1,
        )
    )
    result = uow.execute(request)
    assert result.outcome is CommandOutcome.INSUFFICIENT_FUNDS
    assert result.http_status == 422
    assert uow.fold_stream("a").version == 1
    assert uow.fold_stream("b").version == 1
    assert uow.fold_stream("c").version == 0
    assert uow.execute(request) == result  # persisted rejection replays


def test_post_reordered_legs_under_the_same_command_id_is_a_conflict(
    uow: SqliteCommandUnitOfWork,
) -> None:
    uow.execute(_request(Deposit("a", 10, 0)))
    command_id = uuid4()
    legs = (Leg("a", 10, "debit"), Leg("b", 4, "credit"), Leg("c", 6, "credit"))
    first = uow.execute(_request(Post("a", legs, 1), command_id=command_id))
    assert first.outcome is CommandOutcome.ACCEPTED
    replay = uow.execute(_request(Post("a", legs, 1), command_id=command_id))
    assert replay == first
    reordered = uow.execute(
        _request(Post("a", (legs[0], legs[2], legs[1]), 1), command_id=command_id)
    )
    assert reordered.outcome is CommandOutcome.COMMAND_ID_CONFLICT
    assert uow.fold_stream("b").version == 1  # nothing re-applied


def test_post_and_transfer_are_interchangeable_on_the_log(
    uow: SqliteCommandUnitOfWork, db_path: str
) -> None:
    uow.execute(_request(Deposit("x", 50, 0)))
    uow.execute(_request(Transfer("x", "y", 20, 1)))
    uow.execute(_request(Post("x", (Leg("x", 20, "debit"), Leg("y", 20, "credit")), 2)))
    assert uow.fold_stream("x").balance == 10
    assert uow.fold_stream("y").balance == 40
    feed = SqliteEventStore(db_path)
    try:
        types = [e["type"] for e in feed.read("account-y")]
        assert types == ["TransferCredited", "TransferCredited"]
    finally:
        feed.close()


# -- stream snapshots (ADR-0012) ------------------------------------------------------


def _snapshot_row(db_path: str, account: str):
    with sqlite3.connect(db_path) as conn:
        return conn.execute(
            "SELECT seq, state_json, state_version, anchor_event_id "
            "FROM stream_snapshots WHERE stream = ?",
            (f"account-{account}",),
        ).fetchone()


def _run(uow: SqliteCommandUnitOfWork, account: str, n: int, amount: int = 1) -> int:
    version = uow.fold_stream(account).version
    for _ in range(n):
        result = uow.execute(_request(Deposit(account, amount, version)))
        assert result.outcome is CommandOutcome.ACCEPTED
        version = result.committed_version or 0
    return version


def test_snapshot_written_every_n_events_inside_the_command_transaction(
    db_path: str,
) -> None:
    uow = SqliteCommandUnitOfWork(db_path, snapshot_every=4)
    try:
        account = "acct-snap-a"
        _run(uow, account, 3)
        assert _snapshot_row(db_path, account) is None  # 3 < 4
        _run(uow, account, 1)
        row = _snapshot_row(db_path, account)
        assert row is not None and row[0] == 4
        _run(uow, account, 5)  # 9 events: snapshot advanced to 8
        row = _snapshot_row(db_path, account)
        assert row[0] == 8
        # The snapshot anchors on the real event at seq 8.
        with sqlite3.connect(db_path) as conn:
            (event_id,) = conn.execute(
                "SELECT event_id FROM events WHERE stream = ? AND seq = 8",
                (f"account-{account}",),
            ).fetchone()
        assert row[3] == event_id
        # And the fold it produces equals the fold of the whole stream.
        assert uow.fold_stream(account).balance == 9
    finally:
        uow.close()


def test_fold_from_snapshot_equals_full_fold_and_serves_the_next_decision(
    db_path: str,
) -> None:
    fast = SqliteCommandUnitOfWork(db_path, snapshot_every=2)
    try:
        account = "acct-snap-b"
        version = _run(fast, account, 7, amount=10)
        withdraw = fast.execute(_request(Withdraw(account, 65, version)))
        assert withdraw.outcome is CommandOutcome.ACCEPTED
        rejected = fast.execute(
            _request(Withdraw(account, 6, withdraw.committed_version or 0))
        )
        assert rejected.outcome is CommandOutcome.INSUFFICIENT_FUNDS  # 70-65 = 5
    finally:
        fast.close()
    # A reader with snapshots disabled folds the full stream and agrees.
    plain = SqliteCommandUnitOfWork(db_path, snapshot_every=0)
    try:
        assert plain.fold_stream(account).balance == 5
    finally:
        plain.close()


def test_anchor_mismatch_discards_the_snapshot_and_refolds(
    db_path: str, caplog
) -> None:
    uow = SqliteCommandUnitOfWork(db_path, snapshot_every=2)
    try:
        account = "acct-snap-c"
        _run(uow, account, 4, amount=5)
        with sqlite3.connect(db_path) as conn:
            # Simulate a restore from a different log: the anchor id differs and
            # the cached balance is wrong. Only the log may be believed.
            conn.execute(
                "UPDATE stream_snapshots SET anchor_event_id = 'not-the-event', "
                'state_json = \'{"account_id":"acct-snap-c","balance":999,'
                '"version":4}\' WHERE stream = ?',
                (f"account-{account}",),
            )
        with caplog.at_level("WARNING", logger="cloudscale.snapshots"):
            state = uow.fold_stream(account)
        assert state.balance == 20 and state.version == 4
        assert any("anchor mismatch" in r.getMessage() for r in caplog.records)
    finally:
        uow.close()


def test_stale_state_version_discards_the_snapshot(db_path: str, caplog) -> None:
    uow = SqliteCommandUnitOfWork(db_path, snapshot_every=1)
    try:
        account = "acct-snap-d"
        _run(uow, account, 2, amount=5)
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "UPDATE stream_snapshots SET state_version = state_version + 1, "
                'state_json = \'{"account_id":"acct-snap-d","balance":999,'
                '"version":2}\' WHERE stream = ?',
                (f"account-{account}",),
            )
        with caplog.at_level("WARNING", logger="cloudscale.snapshots"):
            assert uow.fold_stream(account).balance == 10
        assert any("state_version" in r.getMessage() for r in caplog.records)
        # The next accepted command rewrites a fresh, current-version snapshot.
        _run(uow, account, 1)
        row = _snapshot_row(db_path, account)
        assert row[0] == 3 and row[2] == CURRENT_STATE_VERSION
    finally:
        uow.close()


def test_snapshot_upsert_is_monotonic(db_path: str) -> None:
    uow = SqliteCommandUnitOfWork(db_path, snapshot_every=1)
    try:
        account = "acct-snap-e"
        _run(uow, account, 5)
        assert _snapshot_row(db_path, account)[0] == 5
        # A lagging writer trying to store seq 3 must not move it backwards.
        from cloudscale.application.snapshots import StreamSnapshot
        from cloudscale.domain.account import AccountState

        uow._write_snapshot(
            f"account-{account}",
            StreamSnapshot(3, AccountState(account, 3, 3), 1, "old-anchor"),
        )
        uow._conn.commit()
        assert _snapshot_row(db_path, account)[0] == 5
    finally:
        uow.close()


def test_snapshot_every_zero_disables_writing(db_path: str) -> None:
    uow = SqliteCommandUnitOfWork(db_path, snapshot_every=0)
    try:
        _run(uow, "acct-snap-f", 6)
        assert _snapshot_row(db_path, "acct-snap-f") is None
    finally:
        uow.close()


def test_transfer_snapshots_both_streams_independently(db_path: str) -> None:
    uow = SqliteCommandUnitOfWork(db_path, snapshot_every=2)
    try:
        version = _run(uow, "acct-snap-src", 1, amount=100)  # seq 1, no snapshot yet
        result = uow.execute(
            _request(Transfer("acct-snap-src", "acct-snap-dst", 40, version))
        )
        assert result.outcome is CommandOutcome.ACCEPTED
        assert _snapshot_row(db_path, "acct-snap-src")[0] == 2  # source hit 2
        assert _snapshot_row(db_path, "acct-snap-dst") is None  # target at 1
        assert uow.fold_stream("acct-snap-src").balance == 60
        assert uow.fold_stream("acct-snap-dst").balance == 40
    finally:
        uow.close()


# -- holds (ADR-0014) ------------------------------------------------------------------

HOUR = 3600


def _uow_with_clock(db_path: str, moments: list) -> SqliteCommandUnitOfWork:
    """A unit of work whose clock pops from ``moments`` (last one sticks)."""
    from datetime import datetime

    def clock() -> datetime:
        return moments.pop(0) if len(moments) > 1 else moments[0]

    return SqliteCommandUnitOfWork(db_path, clock=clock)


def test_hold_reserves_available_funds_and_is_derived_from_the_log(
    uow: SqliteCommandUnitOfWork,
) -> None:
    uow.execute(_request(Deposit("src", 100, 0)))
    hold_id = uuid4()
    placed = uow.execute(_request(Hold("src", "dst", 40, 1, HOUR), command_id=hold_id))
    assert placed.outcome is CommandOutcome.ACCEPTED
    state = uow.fold_stream("src")
    assert (state.balance, state.held, state.available) == (100, 40, 60)
    assert uow.fold_stream("dst").version == 0  # the target learns nothing yet

    hold = uow.open_hold("src", hold_id)
    assert hold is not None and (hold.amount, hold.counterparty) == (40, "dst")
    assert uow.open_hold("src", uuid4()) is None
    assert uow.open_hold("dst", hold_id) is None  # a hold lives on its source stream

    # Available, not balance, bounds every later debit.
    short = uow.execute(_request(Withdraw("src", 61, 2)))
    assert short.outcome is CommandOutcome.INSUFFICIENT_FUNDS
    ok = uow.execute(_request(Withdraw("src", 60, 2)))
    assert ok.outcome is CommandOutcome.ACCEPTED


def test_post_hold_moves_reserved_funds_to_the_target_in_one_transaction(
    uow: SqliteCommandUnitOfWork,
) -> None:
    uow.execute(_request(Deposit("src", 100, 0)))
    hold_id = uuid4()
    uow.execute(_request(Hold("src", "dst", 40, 1, HOUR), command_id=hold_id))
    result = uow.execute(_request(PostHold("src", hold_id, 2)))
    assert result.outcome is CommandOutcome.ACCEPTED
    assert [(p.account_id, p.committed_version) for p in result.postings] == [
        ("dst", 1),
        ("src", 3),
    ]
    assert result.committed_version == 3
    src, dst = uow.fold_stream("src"), uow.fold_stream("dst")
    assert (src.balance, src.held) == (60, 0)
    assert dst.balance == 40
    assert uow.open_hold("src", hold_id) is None
    # Posting the same hold again is hold_not_open, persisted like any rejection.
    again = uow.execute(_request(PostHold("src", hold_id, 3)))
    assert again.outcome is CommandOutcome.DOMAIN_REJECTED
    assert again.error_code == "hold_not_open"


def test_partial_capture_puts_two_events_on_the_source_and_reports_both(
    uow: SqliteCommandUnitOfWork, db_path: str
) -> None:
    uow.execute(_request(Deposit("src", 100, 0)))
    hold_id = uuid4()
    uow.execute(_request(Hold("src", "dst", 40, 1, HOUR), command_id=hold_id))
    result = uow.execute(_request(PostHold("src", hold_id, 2, amount=15)))
    assert result.outcome is CommandOutcome.ACCEPTED
    # One posting per stream; the source's names its LAST event (seq 4).
    assert [(p.account_id, p.committed_version) for p in result.postings] == [
        ("dst", 1),
        ("src", 4),
    ]
    assert result.committed_version == 4  # the source's last leg
    src = uow.fold_stream("src")
    assert (src.balance, src.held, src.available, src.version) == (85, 0, 85, 4)
    feed = SqliteEventStore(db_path)
    try:
        types = [e["type"] for e in feed.read("account-src")]
        assert types == ["Deposited", "HoldPlaced", "HoldPosted", "HoldReleased"]
        released = feed.read("account-src")[-1]
        assert released["release_reason"] == "partial" and released["amount"] == 25
    finally:
        feed.close()


def test_void_and_expire_release_without_moving_funds(db_path: str) -> None:
    from datetime import UTC, datetime

    t0 = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    uow = _uow_with_clock(db_path, [t0])
    try:
        uow.execute(_request(Deposit("src", 100, 0)))
        voided_id, expiring_id = uuid4(), uuid4()
        uow.execute(_request(Hold("src", "dst", 30, 1, 1800), command_id=voided_id))
        uow.execute(_request(Hold("src", "dst", 20, 2, 1800), command_id=expiring_id))
        assert uow.fold_stream("src").held == 50

        voided = uow.execute(_request(VoidHold("src", voided_id, 3)))
        assert voided.outcome is CommandOutcome.ACCEPTED
        assert uow.fold_stream("src").held == 20

        # Before expiry the sweeper's command is refused; the clock has not moved.
        early = uow.execute(_request(ExpireHold("src", expiring_id, 4)))
        assert early.error_code == "hold_not_expired"
    finally:
        uow.close()
    # A later process (clock past expiry) can expire it; a post is now refused.
    late = _uow_with_clock(db_path, [t0.replace(hour=13)])
    try:
        refused = late.execute(_request(PostHold("src", expiring_id, 4)))
        assert refused.error_code == "hold_expired"
        expired = late.execute(_request(ExpireHold("src", expiring_id, 4)))
        assert expired.outcome is CommandOutcome.ACCEPTED
        state = late.fold_stream("src")
        assert (state.balance, state.held, state.available) == (100, 0, 100)
        assert late.fold_stream("dst").version == 0  # never touched
    finally:
        late.close()


def test_hold_events_snapshot_and_refold_identically(db_path: str) -> None:
    fast = SqliteCommandUnitOfWork(db_path, snapshot_every=2)
    try:
        fast.execute(_request(Deposit("src", 100, 0)))
        hold_id = uuid4()
        fast.execute(
            _request(
                Hold("src", "dst", 40, 1, HOUR),
                command_id=hold_id,
            )
        )
        fast.execute(_request(Withdraw("src", 10, 2)))
        fast.execute(_request(PostHold("src", hold_id, 3, amount=25)))
        snap_state = fast.fold_stream("src")
    finally:
        fast.close()
    plain = SqliteCommandUnitOfWork(db_path, snapshot_every=0)
    try:
        full = plain.fold_stream("src")
    finally:
        plain.close()
    assert snap_state == full
    assert (full.balance, full.held, full.version) == (65, 0, 5)


# -- reverts (ADR-0015) ----------------------------------------------------------------


def _committed_transfer(uow: SqliteCommandUnitOfWork, amount: int = 40) -> UUID:
    uow.execute(_request(Deposit("src", 100, 0)))
    transfer_id = uuid4()
    result = uow.execute(
        _request(Transfer("src", "dst", amount, 1), command_id=transfer_id)
    )
    assert result.outcome is CommandOutcome.ACCEPTED
    return transfer_id


def test_revert_appends_the_mirror_atomically_and_links_it(
    uow: SqliteCommandUnitOfWork, db_path: str
) -> None:
    transfer_id = _committed_transfer(uow)
    revert_id = uuid4()
    result = uow.execute(_request(Revert("src", transfer_id, 2), command_id=revert_id))
    assert result.outcome is CommandOutcome.ACCEPTED
    assert {p.account_id: p.committed_version for p in result.postings} == {
        "src": 3,
        "dst": 2,
    }
    assert uow.fold_stream("src").balance == 100
    assert uow.fold_stream("dst").balance == 0
    assert uow.reverted_by(transfer_id) == revert_id
    legs = uow.legs_of(revert_id)
    assert {type(e).__name__ for e in legs} == {"ReversalDebited", "ReversalCredited"}
    assert all(e.reverts == transfer_id for e in legs)  # type: ignore[union-attr]
    feed = SqliteEventStore(db_path)
    try:
        rows = [e for e in feed.read_all(0) if e["type"].startswith("Reversal")]
        assert {e["reverts"] for e in rows} == {str(transfer_id)}
        assert {e["transfer_id"] for e in rows} == {str(revert_id)}
    finally:
        feed.close()


def test_second_revert_is_already_reverted_and_persisted(
    uow: SqliteCommandUnitOfWork,
) -> None:
    transfer_id = _committed_transfer(uow)
    uow.execute(_request(Revert("src", transfer_id, 2)))
    request = _request(Revert("src", transfer_id, 3))
    again = uow.execute(request)
    assert again.outcome is CommandOutcome.DOMAIN_REJECTED
    assert (again.error_code, again.http_status) == ("already_reverted", 409)
    assert uow.fold_stream("src").version == 3  # nothing appended
    assert uow.execute(request) == again  # replays the rejection


def test_revert_when_the_payee_spent_the_money_appends_nothing(
    uow: SqliteCommandUnitOfWork,
) -> None:
    transfer_id = _committed_transfer(uow)
    uow.execute(_request(Withdraw("dst", 1, 1)))  # dst now has 39
    result = uow.execute(_request(Revert("src", transfer_id, 2)))
    assert result.outcome is CommandOutcome.INSUFFICIENT_FUNDS
    assert result.http_status == 422
    assert uow.fold_stream("src").version == 2 and uow.fold_stream("dst").version == 2
    assert uow.reverted_by(transfer_id) is None


def test_reverting_a_deposit_or_unknown_id_is_not_revertible(
    uow: SqliteCommandUnitOfWork,
) -> None:
    deposit_id = uuid4()
    uow.execute(_request(Deposit("src", 100, 0), command_id=deposit_id))
    result = uow.execute(_request(Revert("src", deposit_id, 1)))
    assert (result.error_code, result.http_status) == ("not_revertible", 400)
    unknown = uow.execute(_request(Revert("src", uuid4(), 1)))
    assert unknown.error_code == "not_revertible"


def test_revert_of_a_revert_and_the_read_model(
    uow: SqliteCommandUnitOfWork, db_path: str, tmp_path
) -> None:
    transfer_id = _committed_transfer(uow)
    first = uuid4()
    uow.execute(_request(Revert("src", transfer_id, 2), command_id=first))
    second = uuid4()
    result = uow.execute(_request(Revert("dst", first, 2), command_id=second))
    assert result.outcome is CommandOutcome.ACCEPTED
    assert uow.fold_stream("src").balance == 60 and uow.fold_stream("dst").balance == 40

    from cloudscale.adapters.sqlite_compat.dead_letter_store import (
        DeadLetteringProjectionStore,
    )
    from cloudscale.processes.resilient_consumer import ResilientConsumer

    feed = SqliteEventStore(db_path)
    projection = DeadLetteringProjectionStore(path=str(tmp_path / "proj.db"))
    try:
        ResilientConsumer(feed, projection).run()
        original = projection.transfer(str(transfer_id))
        assert original["kind"] == "transfer" and original["reverted_by"] == str(first)
        assert {(leg["account_id"], leg["direction"]) for leg in original["legs"]} == {
            ("src", "debit"),
            ("dst", "credit"),
        }
        first_row = projection.transfer(str(first))
        assert first_row["kind"] == "reversal"
        assert first_row["reverts"] == str(transfer_id)
        assert first_row["reverted_by"] == str(second)
        assert projection.transfer(str(second))["reverted_by"] is None
        assert projection.balance("src")["balance"] == 60
    finally:
        projection.close()
        feed.close()
