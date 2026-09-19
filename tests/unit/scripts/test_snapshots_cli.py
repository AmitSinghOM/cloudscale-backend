"""``scripts/snapshots.py`` works on both tiers and never creates schema (ADR-0012).

Run as a subprocess exactly as an operator would. PostgreSQL cases skip when
no server is reachable; the SQLite cases always run.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest

from cloudscale.adapters.sqlite_compat.command_unit_of_work import (
    SqliteCommandUnitOfWork,
)
from cloudscale.application.command_service import normalize_command
from cloudscale.domain.commands import Deposit

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "snapshots.py"
_ADMIN_DSN = os.environ.get("CLOUDSCALE_TEST_PG", "postgresql://localhost/postgres")


def _cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 -- our own script, fixed argv
        [sys.executable, str(SCRIPT), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def _deposits(uow, account: str, n: int) -> None:
    version = 0
    for _ in range(n):
        result = uow.execute(
            normalize_command(
                Deposit(account, 1, version),
                command_id=uuid4(),
                correlation_id=uuid4(),
                issuer="cloudscale",
                subject="u",
            )
        )
        version = result.committed_version or 0


def test_sqlite_stats_and_drop(tmp_path: Path) -> None:
    path = str(tmp_path / "log.db")
    uow = SqliteCommandUnitOfWork(path, snapshot_every=2)
    try:
        _deposits(uow, "a", 5)
        _deposits(uow, "b", 3)
    finally:
        uow.close()

    stats = _cli(path, "stats")
    assert stats.returncode == 0, stats.stderr
    assert "snapshots: 2" in stats.stdout and "streams_with_events: 2" in stats.stdout

    dropped = _cli(path, "drop", "a")
    assert dropped.returncode == 0 and "dropped 1 snapshot(s)" in dropped.stdout
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM stream_snapshots").fetchone()[0] == 1

    # After a drop the next fold is a full fold and still agrees with the log.
    uow = SqliteCommandUnitOfWork(path, snapshot_every=2)
    try:
        assert uow.fold_stream("a").balance == 5
    finally:
        uow.close()

    everything = _cli(path, "drop", "--all")
    assert everything.returncode == 0 and "dropped 1 snapshot(s)" in everything.stdout


def test_sqlite_missing_database_is_exit_2(tmp_path: Path) -> None:
    result = _cli(str(tmp_path / "nope.db"), "stats")
    assert result.returncode == 2 and "database not found" in result.stderr


def test_drop_requires_exactly_one_target(tmp_path: Path) -> None:
    path = str(tmp_path / "log.db")
    SqliteCommandUnitOfWork(path).close()
    assert _cli(path, "drop").returncode == 2
    assert _cli(path, "drop", "a", "--all").returncode == 2


# -- PostgreSQL ---------------------------------------------------------------------

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
    database = f"cloudscale_snap_{uuid.uuid4().hex[:12]}"
    admin = psycopg.connect(_ADMIN_DSN, autocommit=True)
    admin.execute(f'CREATE DATABASE "{database}"')
    try:
        yield f"{_ADMIN_DSN.rsplit('/', 1)[0]}/{database}"
    finally:
        admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
        admin.close()


@needs_pg
def test_postgres_refuses_an_unmigrated_database_and_creates_nothing(
    fresh_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CLOUDSCALE_PG_SCHEMA", raising=False)
    result = _cli(fresh_dsn, "stats")
    assert result.returncode == 2, result.stdout
    assert "not a migrated cloudscale database" in result.stderr
    with psycopg.connect(fresh_dsn) as conn:
        assert (
            conn.execute("SELECT to_regclass('stream_snapshots')").fetchone()[0] is None
        )


@needs_pg
def test_postgres_stats_and_drop_after_migration(
    fresh_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from alembic import command

    from cloudscale.adapters.postgres.command_unit_of_work import (
        PostgresCommandUnitOfWork,
    )
    from cloudscale.entrypoints.migrate import alembic_config

    monkeypatch.setenv("CLOUDSCALE_PG_DSN", fresh_dsn)
    command.upgrade(alembic_config(), "head")
    monkeypatch.setenv("CLOUDSCALE_PG_SCHEMA", "migrations")
    uow = PostgresCommandUnitOfWork(fresh_dsn, snapshot_every=2, pool_max=1)
    try:
        _deposits(uow, "a", 4)
    finally:
        uow.close()

    stats = _cli(fresh_dsn, "stats")
    assert stats.returncode == 0, stats.stderr
    assert "snapshots: 1" in stats.stdout
    dropped = _cli(fresh_dsn, "drop", "--all")
    assert dropped.returncode == 0 and "dropped 1 snapshot(s)" in dropped.stdout
