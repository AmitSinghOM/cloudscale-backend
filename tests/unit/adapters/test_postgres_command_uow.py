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
from cloudscale.domain.commands import (  # noqa: E402
    Deposit,
    Hold,
    Leg,
    Post,
    PostHold,
    Transfer,
    VoidHold,
    Withdraw,
)
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
        return _BoundStorage(conn, snapshot_every=0).fold_stream(account_id)


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


# -- stream snapshots (ADR-0012) ------------------------------------------------------


def _snapshot_row(dsn: str, account: str):
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            "SELECT seq, state_json, state_version, anchor_event_id "
            "FROM stream_snapshots WHERE stream = %s",
            (f"account-{account}",),
        ).fetchone()


def _run(uow: PostgresCommandUnitOfWork, account: str, n: int, amount: int = 1) -> int:
    version = _state(uow._pool.conninfo, account).version
    for _ in range(n):
        result = uow.execute(_request(Deposit(account, amount, version)))
        assert result.outcome is CommandOutcome.ACCEPTED
        version = result.committed_version or 0
    return version


def test_snapshot_written_every_n_events_and_anchored_on_the_log(
    throwaway_dsn: str, account: str
) -> None:
    uow = PostgresCommandUnitOfWork(throwaway_dsn, snapshot_every=4)
    try:
        _run(uow, account, 3)
        assert _snapshot_row(throwaway_dsn, account) is None
        _run(uow, account, 6)  # 9 events -> snapshot at 8
        row = _snapshot_row(throwaway_dsn, account)
        assert row is not None and row[0] == 8
        with psycopg.connect(throwaway_dsn) as conn:
            (event_id,) = conn.execute(
                "SELECT event_id FROM events WHERE stream = %s AND seq = 8",
                (f"account-{account}",),
            ).fetchone()
        assert row[3] == event_id
        assert _state(throwaway_dsn, account).balance == 9
    finally:
        uow.close()


def test_fold_from_snapshot_serves_decisions_identically_to_the_full_fold(
    throwaway_dsn: str, account: str
) -> None:
    fast = PostgresCommandUnitOfWork(throwaway_dsn, snapshot_every=2)
    try:
        version = _run(fast, account, 7, amount=10)
        assert _snapshot_row(throwaway_dsn, account)[0] == 6
        withdraw = fast.execute(_request(Withdraw(account, 65, version)))
        assert withdraw.outcome is CommandOutcome.ACCEPTED
        rejected = fast.execute(
            _request(Withdraw(account, 6, withdraw.committed_version or 0))
        )
        assert rejected.outcome is CommandOutcome.INSUFFICIENT_FUNDS
    finally:
        fast.close()
    # _state() folds with snapshots disabled -> full fold from seq 1.
    assert _state(throwaway_dsn, account).balance == 5


def test_anchor_mismatch_discards_the_snapshot_on_postgres(
    throwaway_dsn: str, account: str, caplog
) -> None:
    uow = PostgresCommandUnitOfWork(throwaway_dsn, snapshot_every=2)
    try:
        _run(uow, account, 4, amount=5)
        with psycopg.connect(throwaway_dsn, autocommit=True) as conn:
            conn.execute(
                "UPDATE stream_snapshots SET anchor_event_id = 'not-the-event', "
                "state_json = %s WHERE stream = %s",
                (
                    '{"account_id":"%s","balance":999,"version":4}' % account,
                    f"account-{account}",
                ),
            )
        with caplog.at_level("WARNING", logger="cloudscale.snapshots"):
            version = _run(uow, account, 1, amount=5)  # decision refolds from the log
        assert version == 5
        assert any("anchor mismatch" in r.getMessage() for r in caplog.records)
        assert _state(throwaway_dsn, account).balance == 25
    finally:
        uow.close()


def test_snapshot_upsert_is_monotonic_on_postgres(
    throwaway_dsn: str, account: str
) -> None:
    from cloudscale.application.snapshots import StreamSnapshot
    from cloudscale.domain.account import AccountState

    uow = PostgresCommandUnitOfWork(throwaway_dsn, snapshot_every=1)
    try:
        _run(uow, account, 5)
        assert _snapshot_row(throwaway_dsn, account)[0] == 5
        with psycopg.connect(throwaway_dsn, row_factory=psycopg.rows.dict_row) as conn:
            _BoundStorage(conn, snapshot_every=1)._write_snapshot(
                f"account-{account}",
                StreamSnapshot(3, AccountState(account, 3, 3), 1, "old-anchor"),
            )
            conn.commit()
        assert _snapshot_row(throwaway_dsn, account)[0] == 5
    finally:
        uow.close()


def test_snapshot_every_zero_disables_writing_on_postgres(
    throwaway_dsn: str, account: str
) -> None:
    uow = PostgresCommandUnitOfWork(throwaway_dsn, snapshot_every=0)
    try:
        _run(uow, account, 6)
        assert _snapshot_row(throwaway_dsn, account) is None
    finally:
        uow.close()


def test_concurrent_writers_on_one_deep_stream_keep_snapshot_and_fold_consistent(
    throwaway_dsn: str, account: str
) -> None:
    """Retries refold on a fresh tracker; the snapshot never lies about the log."""
    uow = PostgresCommandUnitOfWork(throwaway_dsn, snapshot_every=3, pool_max=4)
    try:
        _run(uow, account, 5, amount=1)
        outcomes: list[CommandOutcome] = []
        lock = threading.Lock()

        def racer() -> None:
            version = _state(throwaway_dsn, account).version
            result = uow.execute(_request(Deposit(account, 1, version)))
            with lock:
                outcomes.append(result.outcome)

        threads = [threading.Thread(target=racer) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        accepted = outcomes.count(CommandOutcome.ACCEPTED)
        assert accepted >= 1
        state = _state(throwaway_dsn, account)
        assert state.balance == 5 + accepted
        row = _snapshot_row(throwaway_dsn, account)
        assert row is not None and row[0] <= state.version
        # The snapshot's cached state matches the log at its own seq.
        with psycopg.connect(throwaway_dsn) as conn:
            (count,) = conn.execute(
                "SELECT COUNT(*) FROM events WHERE stream = %s AND seq <= %s",
                (f"account-{account}", row[0]),
            ).fetchone()
        assert count == row[0]
    finally:
        uow.close()


# -- N-leg postings (ADR-0013) ---------------------------------------------------------


def test_three_way_cyclic_postings_never_deadlock(
    throwaway_dsn: str, account: str
) -> None:
    """A->{B,C}, B->{C,A}, C->{A,B} racing form a 3-cycle over the same keys.

    Pairwise ordering is not enough for a cycle of three; the total
    account-id order is. A deadlock would surface as psycopg
    DeadlockDetected out of the unit of work; the accepted outcomes here are
    ACCEPTED or a VERSION_CONFLICT on the racer's own anchor version.
    """
    a, b, c = (f"{account}-{n}" for n in "abc")
    seed = PostgresCommandUnitOfWork(throwaway_dsn)
    for name in (a, b, c):
        seed.execute(_request(Deposit(name, 1_000, 0)))
    seed.close()

    def post(anchor: str, x: str, y: str, version: int) -> NormalizedCommand:
        return _request(
            Post(
                anchor,
                (Leg(anchor, 6, "debit"), Leg(x, 4, "credit"), Leg(y, 2, "credit")),
                version,
            )
        )

    for _ in range(10):
        va, vb, vc = (_state(throwaway_dsn, n).version for n in (a, b, c))
        outcomes = _race(
            throwaway_dsn,
            [post(a, b, c, va), post(b, c, a, vb), post(c, a, b, vc)],
            rounds=1,
        )[0]
        assert all(
            o in (CommandOutcome.ACCEPTED, CommandOutcome.VERSION_CONFLICT)
            for o in outcomes
        ), outcomes
        assert CommandOutcome.ACCEPTED in outcomes
    total = sum(_state(throwaway_dsn, n).balance for n in (a, b, c))
    assert total == 3_000


def test_post_legs_project_into_every_balance_on_postgres(
    throwaway_dsn: str, account: str
) -> None:
    payer, m, f = f"{account}-p", f"{account}-m", f"{account}-f"
    uow = PostgresCommandUnitOfWork(throwaway_dsn)
    try:
        uow.execute(_request(Deposit(payer, 100, 0)))
        result = uow.execute(
            _request(
                Post(
                    payer,
                    (
                        Leg(payer, 100, "debit"),
                        Leg(m, 97, "credit"),
                        Leg(f, 3, "credit"),
                    ),
                    1,
                )
            )
        )
        assert result.outcome is CommandOutcome.ACCEPTED
        assert len(result.postings) == 3
    finally:
        uow.close()
    assert _state(throwaway_dsn, payer).balance == 0
    assert _state(throwaway_dsn, m).balance == 97
    assert _state(throwaway_dsn, f).balance == 3
    feed = PostgresEventStore(throwaway_dsn)
    try:
        assert (
            len(
                [
                    e
                    for e in feed.read(f"account-{payer}")
                    if e["type"] == "TransferDebited"
                ]
            )
            == 1
        )
    finally:
        feed.close()


# -- holds (ADR-0014) ------------------------------------------------------------------


def test_hold_lifecycle_on_postgres_and_read_model(
    throwaway_dsn: str, account: str
) -> None:
    from cloudscale.adapters.postgres.projection_store import PostgresProjectionStore
    from cloudscale.processes.resilient_consumer import ResilientConsumer

    src, dst = f"{account}-src", f"{account}-dst"
    uow = PostgresCommandUnitOfWork(throwaway_dsn)
    try:
        uow.execute(_request(Deposit(src, 100, 0)))
        hold_id = uuid.uuid4()
        placed = uow.execute(_request(Hold(src, dst, 40, 1, 3600), command_id=hold_id))
        assert placed.outcome is CommandOutcome.ACCEPTED
        assert _state(throwaway_dsn, src).held == 40
        hold = _BoundStorageProbe(throwaway_dsn).open_hold(src, hold_id)
        assert hold is not None and hold.counterparty == dst

        posted = uow.execute(_request(PostHold(src, hold_id, 2, amount=25)))
        assert posted.outcome is CommandOutcome.ACCEPTED
        assert {p.account_id: p.committed_version for p in posted.postings} == {
            dst: 1,
            src: 4,
        }
        state = _state(throwaway_dsn, src)
        assert (state.balance, state.held, state.available) == (75, 0, 75)
        assert _state(throwaway_dsn, dst).balance == 25
    finally:
        uow.close()

    # The consumer projects held and the per-hold read model.
    feed = PostgresEventStore(throwaway_dsn)
    projection = PostgresProjectionStore(throwaway_dsn, consumer=f"holds-{account}")
    try:
        ResilientConsumer(feed, projection).run()
        balance = projection.balance(src)
        assert (balance["balance"], balance["held"]) == (75, 0)
        with psycopg.connect(throwaway_dsn) as conn:
            (state_,) = conn.execute(
                "SELECT state FROM holds WHERE hold_id = %s", (str(hold_id),)
            ).fetchone()
        assert state_ == "posted"
    finally:
        projection.close()
        feed.close()


class _BoundStorageProbe:
    """Read-only access to the unit of work's derivations on a fresh connection."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def open_hold(self, account_id: str, hold_id: UUID):
        with psycopg.connect(self._dsn, row_factory=psycopg.rows.dict_row) as conn:
            return _BoundStorage(conn, snapshot_every=0).open_hold(account_id, hold_id)


def test_concurrent_post_and_void_of_one_hold_resolve_to_exactly_one_winner(
    throwaway_dsn: str, account: str
) -> None:
    """The hold is derived from the log inside the transaction, so a post and a
    void racing on the same hold cannot both apply."""
    src, dst = f"{account}-s", f"{account}-d"
    seed = PostgresCommandUnitOfWork(throwaway_dsn)
    seed.execute(_request(Deposit(src, 100, 0)))
    hold_id = uuid.uuid4()
    seed.execute(_request(Hold(src, dst, 40, 1, 3600), command_id=hold_id))
    seed.close()

    outcomes = _race(
        throwaway_dsn,
        [_request(PostHold(src, hold_id, 2)), _request(VoidHold(src, hold_id, 2))],
        rounds=1,
    )[0]
    assert sorted(o.value for o in outcomes) == ["accepted", "version_conflict"]
    state = _state(throwaway_dsn, src)
    assert state.held == 0
    # Either the post won (balance 60, dst 40) or the void won (100, 0) -- never both.
    assert (state.balance, _state(throwaway_dsn, dst).balance) in {(60, 40), (100, 0)}
