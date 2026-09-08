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
from cloudscale.domain.commands import Deposit, Withdraw
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
