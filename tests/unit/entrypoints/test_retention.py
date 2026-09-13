"""Retention: bounded growth for idempotency records, limiter buckets, DLQ."""

from __future__ import annotations

import os
import sqlite3
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
from cloudscale.domain.commands import Deposit
from cloudscale.entrypoints.retention import (
    RetentionPolicy,
    main,
    prune_postgres,
    prune_sqlite,
)

NOW = datetime(2026, 9, 13, 12, 0, tzinfo=UTC)


def _deposit(uow: SqliteCommandUnitOfWork, expected_version: int) -> None:
    uow.execute(
        normalize_command(
            Deposit(account_id="acc", amount=5, expected_version=expected_version),
            command_id=uuid.uuid4(),
            correlation_id=uuid.uuid4(),
            issuer="cloudscale",
            subject="u",
        )
    )


def _count(path: str, table: str) -> int:
    conn = sqlite3.connect(path)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
    finally:
        conn.close()


# -- policy ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"command_results_days": 0},
        {"rate_limit_idle_seconds": -1},
        {"dead_letters_days": 0},
        {"batch_size": 0},
    ],
)
def test_policy_rejects_non_positive_values(kwargs) -> None:
    with pytest.raises(ValueError):
        RetentionPolicy(**kwargs)


# -- SQLite ----------------------------------------------------------------------------


def test_sqlite_prunes_only_records_older_than_the_window(tmp_path: Path) -> None:
    log = str(tmp_path / "log.db")
    # The UoW reads the clock more than once per command; pin one moment for
    # the duration of each execute().
    moments = [
        NOW - timedelta(days=10),
        NOW - timedelta(days=8),
        NOW - timedelta(hours=1),
    ]
    current = {"t": moments[0]}
    uow = SqliteCommandUnitOfWork(log, clock=lambda: current["t"])
    for version, moment in enumerate(moments):
        current["t"] = moment
        _deposit(uow, expected_version=version)
    uow.close()
    assert _count(log, "command_results") == 3

    report = prune_sqlite(
        log,
        str(tmp_path / "p.db"),
        RetentionPolicy(command_results_days=7, batch_size=1),
        now=NOW,
    )
    assert report.command_results == 2
    assert report.batches >= 3  # batch_size=1: 2 deleting passes + terminating pass
    assert _count(log, "command_results") == 1
    # The event log itself is never touched.
    assert _count(log, "events") == 3


def test_sqlite_legacy_file_without_created_at_is_upgraded_in_place(
    tmp_path: Path,
) -> None:
    """A database from before retention gains the column; existing rows read as fresh."""
    log = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(log)
    conn.executescript(
        "CREATE TABLE command_results (command_id TEXT PRIMARY KEY, "
        "request_hash BLOB NOT NULL, result_json TEXT NOT NULL);"
        "INSERT INTO command_results VALUES ('old-1', x'00', '{}');"
    )
    conn.commit()
    conn.close()

    SqliteCommandUnitOfWork(log).close()  # opening performs the upgrade
    conn = sqlite3.connect(log)
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(command_results)")}
        assert "created_at" in cols
        stamped = conn.execute(
            "SELECT created_at FROM command_results WHERE command_id='old-1'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert stamped > 0  # stamped at upgrade time, not epoch zero

    # Fresh-stamped legacy rows survive a prune with the default window.
    report = prune_sqlite(log, str(tmp_path / "p.db"), RetentionPolicy())
    assert report.command_results == 0
    assert _count(log, "command_results") == 1


def test_sqlite_dead_letters_kept_unless_ttl_given(tmp_path: Path) -> None:
    projection_path = str(tmp_path / "p.db")
    store = DeadLetteringProjectionStore(path=projection_path)
    bad = {"event_id": "e-old", "type": "Deposited", "account_id": "a", "amount": "NaN"}
    store.dead_letter(bad, ValueError("bad amount"), attempts=1)
    store.dead_letter(
        {**bad, "event_id": "e-new"}, ValueError("bad amount"), attempts=1
    )
    store.close()
    assert _count(projection_path, "dead_letters") == 2

    # Backdate one letter far into the past.
    conn = sqlite3.connect(projection_path)
    conn.execute(
        "UPDATE dead_letters SET dead_lettered_at = ? WHERE event_id = 'e-old'",
        ((NOW - timedelta(days=90)).isoformat(),),
    )
    conn.commit()
    conn.close()

    log = str(tmp_path / "log.db")
    SqliteCommandUnitOfWork(log).close()
    untouched = prune_sqlite(log, projection_path, RetentionPolicy(), now=NOW)
    assert untouched.dead_letters == 0
    assert _count(projection_path, "dead_letters") == 2

    pruned = prune_sqlite(
        log, projection_path, RetentionPolicy(dead_letters_days=30), now=NOW
    )
    assert pruned.dead_letters == 1
    assert _count(projection_path, "dead_letters") == 1


def test_cli_sqlite_prints_a_json_report(tmp_path: Path, capsys, monkeypatch) -> None:
    log, proj = str(tmp_path / "log.db"), str(tmp_path / "p.db")
    SqliteCommandUnitOfWork(log).close()
    DeadLetteringProjectionStore(path=proj).close()
    monkeypatch.setenv("CLOUDSCALE_STORAGE", "sqlite")
    monkeypatch.setenv("CLOUDSCALE_LOG_DB", log)
    monkeypatch.setenv("CLOUDSCALE_PROJECTION_DB", proj)
    assert main(["--command-results-days", "1"]) == 0
    out = capsys.readouterr().out
    assert '"storage": "sqlite"' in out and '"command_results": 0' in out


# -- PostgreSQL ------------------------------------------------------------------------

psycopg = pytest.importorskip("psycopg")
_ADMIN_DSN = os.environ.get("CLOUDSCALE_TEST_PG", "postgresql://localhost/postgres")


def _pg_available() -> bool:
    try:
        with psycopg.connect(_ADMIN_DSN, connect_timeout=2):
            return True
    except psycopg.OperationalError:
        return False


pg = pytest.mark.skipif(not _pg_available(), reason="no PostgreSQL reachable")


@pytest.fixture()
def fresh_dsn() -> Iterator[str]:
    database = f"cloudscale_ret_{uuid.uuid4().hex[:12]}"
    admin = psycopg.connect(_ADMIN_DSN, autocommit=True)
    admin.execute(f'CREATE DATABASE "{database}"')
    try:
        yield f"{_ADMIN_DSN.rsplit('/', 1)[0]}/{database}"
    finally:
        admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
        admin.close()


@pg
def test_pg_prunes_results_buckets_and_optionally_dead_letters(
    fresh_dsn: str, monkeypatch
) -> None:
    from cloudscale.adapters.postgres.command_unit_of_work import (
        PostgresCommandUnitOfWork,
    )
    from cloudscale.adapters.postgres.projection_store import PostgresProjectionStore
    from cloudscale.adapters.postgres.rate_limiter import PostgresRateLimiter

    monkeypatch.setenv("CLOUDSCALE_PG_SCHEMA", "auto")
    uow = PostgresCommandUnitOfWork(fresh_dsn)
    proj = PostgresProjectionStore(fresh_dsn)
    limiter = PostgresRateLimiter(fresh_dsn, 60)
    try:
        for version in range(3):
            uow.execute(
                normalize_command(
                    Deposit(account_id="acc", amount=5, expected_version=version),
                    command_id=uuid.uuid4(),
                    correlation_id=uuid.uuid4(),
                    issuer="cloudscale",
                    subject="u",
                )
            )
        limiter.try_acquire("subject-a")
        limiter.try_acquire("subject-b")
        proj.dead_letter(
            {"event_id": "dl-1", "type": "Deposited", "account_id": "a", "amount": "x"},
            ValueError("bad amount"),
            attempts=1,
        )
    finally:
        uow.close()
        proj.close()
        limiter.close()

    with psycopg.connect(fresh_dsn, autocommit=True) as conn:
        # Backdate two of three results, one bucket, and the dead letter.
        conn.execute(
            "UPDATE command_results SET created_at = now() - interval '10 days' "
            "WHERE command_id IN (SELECT command_id FROM command_results LIMIT 2)"
        )
        conn.execute(
            "UPDATE rate_limit_buckets SET updated_at = updated_at - 7200 "
            "WHERE bucket_key = 'subject-a'"
        )
        conn.execute(
            "UPDATE dead_letters SET dead_lettered_at = %s",
            ((datetime.now(UTC) - timedelta(days=90)).isoformat(),),
        )

    kept = prune_postgres(
        fresh_dsn, RetentionPolicy(command_results_days=7, batch_size=1)
    )
    assert kept.command_results == 2
    assert kept.rate_limit_buckets == 1
    assert kept.dead_letters == 0  # no TTL given: evidence is kept

    pruned = prune_postgres(fresh_dsn, RetentionPolicy(dead_letters_days=30))
    assert pruned.dead_letters == 1

    with psycopg.connect(fresh_dsn) as conn:
        assert conn.execute("SELECT COUNT(*) FROM command_results").fetchone()[0] == 1
        assert (
            conn.execute("SELECT COUNT(*) FROM rate_limit_buckets").fetchone()[0] == 1
        )
        assert (
            conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 3
        )  # untouched


@pg
def test_pg_prune_tolerates_tables_this_deployment_never_created(
    fresh_dsn: str, monkeypatch
) -> None:
    """Only the command UoW has run: no rate_limit_buckets, no dead_letters.
    Retention must prune what exists and report zero for the rest, not crash.
    (Found by the soak harness, which runs without the postgres limiter.)"""
    from cloudscale.adapters.postgres.command_unit_of_work import (
        PostgresCommandUnitOfWork,
    )

    monkeypatch.setenv("CLOUDSCALE_PG_SCHEMA", "auto")
    PostgresCommandUnitOfWork(fresh_dsn).close()
    with psycopg.connect(fresh_dsn) as conn:
        present = conn.execute(
            "SELECT to_regclass('rate_limit_buckets') IS NOT NULL"
        ).fetchone()[0]
    assert present is False  # precondition: the limiter table really is absent

    report = prune_postgres(fresh_dsn, RetentionPolicy(dead_letters_days=1))
    assert report.rate_limit_buckets == 0
    assert report.dead_letters == 0


@pg
def test_pg_upgrade_from_0001_adds_created_at_and_stamps_existing_rows(
    fresh_dsn: str, monkeypatch
) -> None:
    """Databases stamped at 0001 upgrade cleanly; pre-existing rows read as fresh."""
    from alembic import command

    from cloudscale.adapters.postgres import schema
    from cloudscale.entrypoints.migrate import alembic_config

    monkeypatch.setenv("CLOUDSCALE_PG_DSN", fresh_dsn)
    command.upgrade(alembic_config(), "0001_initial")
    with psycopg.connect(fresh_dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO command_results (command_id, request_hash, result_json) "
            "VALUES ('legacy', %s, '{}')",
            (b"\x00",),
        )
    command.upgrade(alembic_config(), "head")
    with psycopg.connect(fresh_dsn) as conn:
        version = conn.execute("SELECT version_num FROM alembic_version").fetchone()[0]
        assert version == schema.CURRENT_REVISION
        age = conn.execute(
            "SELECT now() - created_at FROM command_results WHERE command_id='legacy'"
        ).fetchone()[0]
    assert age < timedelta(minutes=1)
    assert prune_postgres(fresh_dsn, RetentionPolicy()).command_results == 0
