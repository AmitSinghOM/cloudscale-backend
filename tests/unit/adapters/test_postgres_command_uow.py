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
    _BoundStorage,
    PostgresCommandUnitOfWork,
)
from cloudscale.adapters.postgres.event_store import PostgresEventStore  # noqa: E402
from cloudscale.application.command_service import normalize_command  # noqa: E402
from cloudscale.application.ports import NormalizedCommand  # noqa: E402
from cloudscale.domain.commands import Deposit, Transfer, Withdraw  # noqa: E402
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


def _state(dsn: str, account_id: str):
    """Fold one stream exactly as the unit of work does, on a fresh connection."""
    with psycopg.connect(dsn, row_factory=psycopg.rows.dict_row) as conn:
        return _BoundStorage(conn).fold_stream(account_id)


# -- double-entry transfers (ADR-0011) ------------------------------------------------


def test_transfer_commits_both_legs_in_one_transaction(
    throwaway_dsn: str, account: str
) -> None:
    other = f"{account}-peer"
    unit = PostgresCommandUnitOfWork(throwaway_dsn)
    try:
        unit.execute(_request(Deposit(account, 100, 0)))
        result = unit.execute(_request(Transfer(account, other, 30, 1)))
        assert result.outcome is CommandOutcome.ACCEPTED
        assert {p.account_id for p in result.postings} == {account, other}
        assert unit.execute(_request(Transfer(account, other, 1, 1))).outcome is (
            CommandOutcome.VERSION_CONFLICT
        )
        with psycopg.connect(throwaway_dsn) as conn:
            rows = conn.execute(
                "SELECT stream, type, transfer_id, counterparty FROM events "
                "WHERE transfer_id = %s ORDER BY stream",
                (str(result.command_id),),
            ).fetchall()
        assert [(r[1], r[3]) for r in rows] == [
            ("TransferDebited", other),
            ("TransferCredited", account),
        ]
    finally:
        unit.close()


def _race(
    dsn: str, requests: list[NormalizedCommand], rounds: int
) -> list[list[CommandOutcome]]:
    """Run each request on its own connection, released together, ``rounds`` times."""
    units = [PostgresCommandUnitOfWork(dsn) for _ in requests]
    per_round: list[list[CommandOutcome]] = []
    try:
        for _ in range(rounds):
            barrier = threading.Barrier(len(units))
            outcomes: list[CommandOutcome] = []
            collect = threading.Lock()
            fresh = [_request(r.command) for r in requests]

            def contend(
                unit: PostgresCommandUnitOfWork, request: NormalizedCommand
            ) -> None:
                barrier.wait()
                result = unit.execute(request)
                with collect:
                    outcomes.append(result.outcome)

            threads = [
                threading.Thread(target=contend, args=(unit, request))
                for unit, request in zip(units, fresh, strict=True)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            per_round.append(outcomes)
    finally:
        for unit in units:
            unit.close()
    return per_round


def test_opposite_direction_transfers_never_deadlock(
    throwaway_dsn: str, account: str
) -> None:
    """A->B racing B->A inserts the same two (stream, seq) keys.

    Appended in arbitrary order this deadlocks (PostgreSQL 40P01) and one side
    fails spuriously; legs are appended in ascending account order so the
    loser waits on one key, re-folds, and gets a version_conflict (its
    expected_version is now stale) rather than an exception.
    """
    a, b = f"{account}-a", f"{account}-b"
    seed = PostgresCommandUnitOfWork(throwaway_dsn)
    seed.execute(_request(Deposit(a, 1_000, 0)))
    seed.execute(_request(Deposit(b, 1_000, 0)))
    seed.close()

    for _ in range(10):
        va, vb = _state(throwaway_dsn, a).version, _state(throwaway_dsn, b).version
        outcomes = _race(
            throwaway_dsn,
            [_request(Transfer(a, b, 1, va)), _request(Transfer(b, a, 1, vb))],
            rounds=1,
        )[0]
        # Both can win (versions differ per direction) or one loses on its
        # own stale source version; a deadlock would surface as an exception.
        assert all(
            o in (CommandOutcome.ACCEPTED, CommandOutcome.VERSION_CONFLICT)
            for o in outcomes
        )
        assert CommandOutcome.ACCEPTED in outcomes
    total = _state(throwaway_dsn, a).balance + _state(throwaway_dsn, b).balance
    assert total == 2_000  # conservation across every race


def test_target_side_race_self_heals_via_retry_on_fresh_fold(
    throwaway_dsn: str, account: str
) -> None:
    """Two transfers into one target from different sources both succeed.

    The target has no client-supplied version; the second writer's
    UNIQUE (stream, seq) collision is retried on a fresh fold inside the
    unit of work, so neither caller sees a conflict on an account it never
    named a version for.
    """
    s1, s2, target = f"{account}-s1", f"{account}-s2", f"{account}-t"
    seed = PostgresCommandUnitOfWork(throwaway_dsn)
    seed.execute(_request(Deposit(s1, 50, 0)))
    seed.execute(_request(Deposit(s2, 50, 0)))
    seed.close()

    outcomes = _race(
        throwaway_dsn,
        [_request(Transfer(s1, target, 5, 1)), _request(Transfer(s2, target, 7, 1))],
        rounds=1,
    )[0]
    assert outcomes == [CommandOutcome.ACCEPTED, CommandOutcome.ACCEPTED]
    assert _state(throwaway_dsn, target).balance == 12
    assert _state(throwaway_dsn, target).version == 2
