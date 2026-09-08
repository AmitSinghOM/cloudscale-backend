"""Postgres CommandUnitOfWork contract tests, including cross-instance races.

Skipped when no PostgreSQL is reachable (same gating as the tier tests).
The throwaway database is dropped in teardown.
"""

from __future__ import annotations

import os
import threading
import uuid
from uuid import UUID

import pytest

psycopg = pytest.importorskip("psycopg")

from cloudscale.adapters.postgres.command_unit_of_work import (  # noqa: E402
    PostgresCommandUnitOfWork,
)
from cloudscale.adapters.postgres.event_store import PostgresEventStore  # noqa: E402
from cloudscale.application.command_service import normalize_command  # noqa: E402
from cloudscale.application.ports import NormalizedCommand  # noqa: E402
from cloudscale.domain.commands import Deposit, Withdraw  # noqa: E402
from cloudscale.domain.results import CommandOutcome  # noqa: E402

_ADMIN_DSN = os.environ.get("CLOUDSCALE_TEST_PG", "postgresql://localhost/postgres")


def _postgres_available() -> bool:
    try:
        with psycopg.connect(_ADMIN_DSN, connect_timeout=2):
            return True
    except psycopg.OperationalError:
        return False


pytestmark = pytest.mark.skipif(
    not _postgres_available(),
    reason=f"no PostgreSQL reachable at {_ADMIN_DSN}",
)


@pytest.fixture(scope="module")
def throwaway_dsn():
    database = f"cloudscale_uow_{uuid.uuid4().hex[:12]}"
    admin = psycopg.connect(_ADMIN_DSN, autocommit=True)
    admin.execute(f'CREATE DATABASE "{database}"')
    base = _ADMIN_DSN.rsplit("/", 1)[0]
    try:
        yield f"{base}/{database}"
    finally:
        admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
        admin.close()


@pytest.fixture()
def account() -> str:
    return f"acct-{uuid.uuid4().hex[:10]}"


def _request(command, *, command_id: UUID | None = None) -> NormalizedCommand:
    return normalize_command(
        command,
        command_id=command_id or uuid.uuid4(),
        correlation_id=uuid.uuid4(),
        issuer="cloudscale",
        subject="user-1",
    )


def test_accept_replay_conflict_and_rejections(
    throwaway_dsn: str, account: str
) -> None:
    uow = PostgresCommandUnitOfWork(throwaway_dsn)
    try:
        command_id = uuid.uuid4()
        first = uow.execute(_request(Deposit(account, 100, 0), command_id=command_id))
        assert first.outcome is CommandOutcome.ACCEPTED
        assert first.committed_version == 1

        replay = uow.execute(_request(Deposit(account, 100, 0), command_id=command_id))
        assert replay == first

        conflict = uow.execute(
            _request(Deposit(account, 999, 0), command_id=command_id)
        )
        assert conflict.outcome is CommandOutcome.COMMAND_ID_CONFLICT

        stale = uow.execute(_request(Deposit(account, 5, 0)))
        assert stale.outcome is CommandOutcome.VERSION_CONFLICT
        assert stale.http_status == 409

        broke = uow.execute(_request(Withdraw(account, 500, 1)))
        assert broke.outcome is CommandOutcome.INSUFFICIENT_FUNDS
        assert broke.http_status == 422

        # Exactly one event was ever appended for this account.
        feed = PostgresEventStore(throwaway_dsn)
        try:
            assert len(feed.read(f"account-{account}")) == 1
        finally:
            feed.close()
    finally:
        uow.close()


def test_results_survive_reconnect(throwaway_dsn: str, account: str) -> None:
    command_id = uuid.uuid4()
    first = PostgresCommandUnitOfWork(throwaway_dsn)
    original = first.execute(_request(Deposit(account, 42, 0), command_id=command_id))
    first.close()

    second = PostgresCommandUnitOfWork(throwaway_dsn)
    try:
        replay = second.execute(
            _request(Deposit(account, 42, 0), command_id=command_id)
        )
        assert replay == original
    finally:
        second.close()


def test_cross_instance_version_race_yields_one_accept_one_conflict(
    throwaway_dsn: str, account: str
) -> None:
    """Two separate connections (as in two processes) race the same version."""
    seed = PostgresCommandUnitOfWork(throwaway_dsn)
    seed.execute(_request(Deposit(account, 100, 0)))
    seed.close()

    units = [PostgresCommandUnitOfWork(throwaway_dsn) for _ in range(2)]
    barrier = threading.Barrier(2)
    outcomes: list[CommandOutcome] = []
    collect = threading.Lock()

    def contend(unit: PostgresCommandUnitOfWork) -> None:
        request = _request(Withdraw(account, 60, 1))
        barrier.wait()
        result = unit.execute(request)
        with collect:
            outcomes.append(result.outcome)

    threads = [threading.Thread(target=contend, args=(unit,)) for unit in units]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    for unit in units:
        unit.close()

    assert sorted(outcome.value for outcome in outcomes) == [
        "accepted",
        "version_conflict",
    ]


def test_cross_instance_command_id_race_persists_exactly_one_result(
    throwaway_dsn: str, account: str
) -> None:
    """Two connections race the SAME command; one result must win, both see it."""
    command_id = uuid.uuid4()
    units = [PostgresCommandUnitOfWork(throwaway_dsn) for _ in range(2)]
    barrier = threading.Barrier(2)
    results = []
    collect = threading.Lock()

    def contend(unit: PostgresCommandUnitOfWork) -> None:
        request = _request(Deposit(account, 100, 0), command_id=command_id)
        barrier.wait()
        result = unit.execute(request)
        with collect:
            results.append(result)

    threads = [threading.Thread(target=contend, args=(unit,)) for unit in units]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    for unit in units:
        unit.close()

    assert len(results) == 2
    assert results[0] == results[1]  # both callers got the one persisted result
    assert results[0].outcome is CommandOutcome.ACCEPTED

    feed = PostgresEventStore(throwaway_dsn)
    try:
        assert len(feed.read(f"account-{account}")) == 1
    finally:
        feed.close()
