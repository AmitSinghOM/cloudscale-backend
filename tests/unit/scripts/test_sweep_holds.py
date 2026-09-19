"""``scripts/sweep_holds.py``: expiry is a command with a deterministic id (ADR-0014)."""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from cloudscale.adapters.sqlite_compat.command_unit_of_work import (
    SqliteCommandUnitOfWork,
)
from cloudscale.adapters.sqlite_compat.dead_letter_store import (
    DeadLetteringProjectionStore,
)
from cloudscale.application.command_service import normalize_command
from cloudscale.domain.commands import Deposit, Hold
from cloudscale.processes.resilient_consumer import ResilientConsumer
from cqrs import SqliteEventStore

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "sweep_holds.py"
sys.path.insert(0, str(ROOT / "scripts"))
import sweep_holds  # noqa: E402

_ADMIN_DSN = os.environ.get("CLOUDSCALE_TEST_PG", "postgresql://localhost/postgres")


def _request(command, command_id=None):
    return normalize_command(
        command,
        command_id=command_id or uuid.uuid4(),
        correlation_id=uuid.uuid4(),
        issuer="cloudscale",
        subject="u",
    )


def _seed_sqlite(tmp_path: Path, expires_in: timedelta) -> tuple[str, str, uuid.UUID]:
    log, proj = str(tmp_path / "log.db"), str(tmp_path / "proj.db")
    uow = SqliteCommandUnitOfWork(log)
    try:
        uow.execute(_request(Deposit("src", 100, 0)))
        hold_id = uuid.uuid4()
        expires_at = (datetime.now(UTC) + expires_in).isoformat()
        uow.execute(_request(Hold("src", "dst", 40, 1, expires_at), command_id=hold_id))
    finally:
        uow.close()
    feed = SqliteEventStore(log)
    projection = DeadLetteringProjectionStore(path=proj)
    try:
        ResilientConsumer(feed, projection).run()
    finally:
        projection.close()
        feed.close()
    return log, proj, hold_id


def test_expiry_command_id_is_deterministic_per_hold() -> None:
    hold = str(uuid.uuid4())
    assert sweep_holds.expiry_command_id(hold) == sweep_holds.expiry_command_id(hold)
    assert sweep_holds.expiry_command_id(hold) != sweep_holds.expiry_command_id(
        str(uuid.uuid4())
    )


def test_sweeper_expires_only_holds_past_expiry_and_is_idempotent(
    tmp_path: Path,
) -> None:
    log, proj, hold_id = _seed_sqlite(tmp_path, expires_in=timedelta(seconds=-5))
    first = subprocess.run(  # noqa: S603 -- our own script, fixed argv
        [sys.executable, str(SCRIPT), "--log", log, "--projection", proj],
        capture_output=True,
        text=True,
        check=False,
    )
    assert first.returncode == 0, first.stderr
    assert first.stdout.strip() == "expired=1 skipped=0"
    uow = SqliteCommandUnitOfWork(log)
    try:
        state = uow.fold_stream("src")
        assert (state.balance, state.held, state.available) == (100, 0, 100)
        assert uow.open_hold("src", hold_id) is None
    finally:
        uow.close()
    # The read model still says "open" until the consumer runs; a second
    # sweep hits the same deterministic command id and applies nothing.
    second = subprocess.run(  # noqa: S603
        [sys.executable, str(SCRIPT), "--log", log, "--projection", proj],
        capture_output=True,
        text=True,
        check=False,
    )
    assert second.returncode == 0 and second.stdout.strip() == "expired=0 skipped=1"


def test_sweeper_leaves_unexpired_holds_alone(tmp_path: Path) -> None:
    log, proj, _ = _seed_sqlite(tmp_path, expires_in=timedelta(hours=1))
    result = subprocess.run(  # noqa: S603
        [sys.executable, str(SCRIPT), "--log", log, "--projection", proj],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0 and result.stdout.strip() == "expired=0 skipped=0"


def test_sweeper_argument_and_path_errors(tmp_path: Path) -> None:
    missing = subprocess.run(  # noqa: S603
        [
            sys.executable,
            str(SCRIPT),
            "--log",
            str(tmp_path / "x.db"),
            "--projection",
            str(tmp_path / "y.db"),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert missing.returncode == 2 and "database not found" in missing.stderr
    neither = subprocess.run(  # noqa: S603
        [sys.executable, str(SCRIPT)], capture_output=True, text=True, check=False
    )
    assert neither.returncode == 2


# -- PostgreSQL: two sweepers racing ---------------------------------------------------

psycopg = pytest.importorskip("psycopg")


def _postgres_available() -> bool:
    try:
        with psycopg.connect(_ADMIN_DSN, connect_timeout=2):
            return True
    except psycopg.OperationalError:
        return False


needs_pg = pytest.mark.skipif(
    not _postgres_available(), reason=f"no PostgreSQL reachable at {_ADMIN_DSN}"
)


@pytest.fixture()
def fresh_dsn() -> Iterator[str]:
    database = f"cloudscale_sweep_{uuid.uuid4().hex[:12]}"
    admin = psycopg.connect(_ADMIN_DSN, autocommit=True)
    admin.execute(f'CREATE DATABASE "{database}"')
    try:
        yield f"{_ADMIN_DSN.rsplit('/', 1)[0]}/{database}"
    finally:
        admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
        admin.close()


def _seed_expired_holds_pg(dsn: str, count: int) -> list[uuid.UUID]:
    """Deposit 1,000 on ``src`` and place ``count`` holds already past expiry."""
    from cloudscale.adapters.postgres.command_unit_of_work import (
        PostgresCommandUnitOfWork,
    )
    from cloudscale.adapters.postgres.event_store import PostgresEventStore
    from cloudscale.adapters.postgres.projection_store import PostgresProjectionStore

    uow = PostgresCommandUnitOfWork(dsn)
    hold_ids: list[uuid.UUID] = []
    try:
        uow.execute(_request(Deposit("src", 1_000, 0)))
        past = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
        for i in range(count):
            hold_id = uuid.uuid4()
            uow.execute(
                _request(Hold("src", f"dst-{i}", 10, i + 1, past), command_id=hold_id)
            )
            hold_ids.append(hold_id)
        assert uow.fold_stream("src").held == 10 * count
    finally:
        uow.close()
    feed = PostgresEventStore(dsn)
    projection = PostgresProjectionStore(dsn, consumer="sweep-test")
    try:
        ResilientConsumer(feed, projection).run()
    finally:
        projection.close()
        feed.close()
    return hold_ids


def _run_sweeper_pg(dsn: str, results: list, lock: threading.Lock) -> None:
    from cloudscale.adapters.postgres.command_unit_of_work import (
        PostgresCommandUnitOfWork,
    )
    from cloudscale.adapters.postgres.projection_store import PostgresProjectionStore

    unit = PostgresCommandUnitOfWork(dsn, pool_max=1)
    store = PostgresProjectionStore(dsn, pool_max=1, consumer="sweep-test")
    try:
        outcome = sweep_holds.sweep(unit, store)
    finally:
        unit.close()
        store.close()
    with lock:
        results.append(outcome)


@needs_pg
def test_two_sweepers_racing_release_each_expired_hold_exactly_once(
    fresh_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from cloudscale.adapters.postgres.command_unit_of_work import (
        PostgresCommandUnitOfWork,
    )

    monkeypatch.setenv("CLOUDSCALE_PG_SCHEMA", "auto")
    hold_ids = _seed_expired_holds_pg(fresh_dsn, 5)

    results: list[tuple[int, int]] = []
    lock = threading.Lock()
    threads = [
        threading.Thread(target=_run_sweeper_pg, args=(fresh_dsn, results, lock))
        for _ in range(2)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Counters may double-count a replayed ACCEPTED (same command id, same
    # version); the LOG is the exactly-once claim: one release per hold.
    assert sum(r[0] for r in results) >= 5, results
    with psycopg.connect(fresh_dsn) as conn:
        (released,) = conn.execute(
            "SELECT COUNT(*) FROM events WHERE type = 'HoldReleased' "
            "AND release_reason = 'expired'"
        ).fetchone()
    assert released == 5
    check = PostgresCommandUnitOfWork(fresh_dsn)
    try:
        state = check.fold_stream("src")
        assert (state.balance, state.held) == (1_000, 0)
        assert all(check.open_hold("src", h) is None for h in hold_ids)
    finally:
        check.close()
