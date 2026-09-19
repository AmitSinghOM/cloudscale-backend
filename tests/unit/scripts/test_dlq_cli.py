"""RUNBOOK R1 sends operators to ``scripts/dlq.py`` on the production (PostgreSQL) tier.

Review 4 finding: the tool only opened SQLite files, so ``dlq.py <dsn> list``
answered "database not found" on exactly the tier where the poison-event
recovery procedure matters. These tests run the CLI as an operator would.
Skipped when no PostgreSQL is reachable at ``CLOUDSCALE_TEST_PG``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

from alembic import command  # noqa: E402

from cloudscale.adapters.postgres.projection_store import (  # noqa: E402
    PostgresProjectionStore,
)
from cloudscale.entrypoints.migrate import alembic_config  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
DLQ = ROOT / "scripts" / "dlq.py"
_ADMIN_DSN = os.environ.get("CLOUDSCALE_TEST_PG", "postgresql://localhost/postgres")


def _postgres_available() -> bool:
    try:
        with psycopg.connect(_ADMIN_DSN, connect_timeout=2):
            return True
    except psycopg.OperationalError:
        return False


pytestmark = pytest.mark.skipif(
    not _postgres_available(), reason=f"no PostgreSQL reachable at {_ADMIN_DSN}"
)


@pytest.fixture()
def fresh_dsn() -> Iterator[str]:
    database = f"cloudscale_dlq_{uuid.uuid4().hex[:12]}"
    admin = psycopg.connect(_ADMIN_DSN, autocommit=True)
    admin.execute(f'CREATE DATABASE "{database}"')
    try:
        yield f"{_ADMIN_DSN.rsplit('/', 1)[0]}/{database}"
    finally:
        admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
        admin.close()


def _dlq(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 -- our own script, fixed argv
        [sys.executable, str(DLQ), *args], capture_output=True, text=True, check=False
    )


def _migrate(dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLOUDSCALE_PG_DSN", dsn)
    command.upgrade(alembic_config(), "head")


def test_operator_can_list_show_and_redrive_on_the_postgres_tier(
    fresh_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    _migrate(fresh_dsn, monkeypatch)
    monkeypatch.setenv("CLOUDSCALE_PG_SCHEMA", "migrations")
    store = PostgresProjectionStore(fresh_dsn, pool_max=1)
    try:
        parked = store.dead_letter(
            {
                "event_id": "probe-1",
                "id": 1,
                "type": "Deposited",
                "account_id": "p",
                "amount": 5,
            },
            RuntimeError("simulated transient failure"),
            attempts=3,
        )
        assert parked and store.dead_letter_count() == 1
    finally:
        store.close()

    listed = _dlq(fresh_dsn, "list", "--json")
    assert listed.returncode == 0, listed.stderr
    [letter] = json.loads(listed.stdout)
    assert letter["event_id"] == "probe-1" and letter["error_type"] == "RuntimeError"

    shown = _dlq(fresh_dsn, "show", "probe-1")
    assert shown.returncode == 0 and "simulated transient failure" in shown.stdout

    redriven = _dlq(fresh_dsn, "redrive", "--all")
    assert redriven.returncode == 0, redriven.stderr
    assert "applied" in redriven.stdout and "still parked: 0" in redriven.stdout

    with psycopg.connect(fresh_dsn) as conn:
        balance = conn.execute(
            "SELECT balance FROM balances WHERE account_id = 'p'"
        ).fetchone()
    assert balance is not None and int(balance[0]) == 5, "redrive must apply the event"


def test_missing_sqlite_path_is_still_reported_as_not_found(tmp_path: Path) -> None:
    result = _dlq(str(tmp_path / "absent.db"), "list")
    assert result.returncode == 2 and "database not found" in result.stderr


def test_cli_never_creates_schema_in_an_unmigrated_database(
    fresh_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ADR-0007: an operator tool pointed at the wrong (empty) database must
    refuse, not quietly create the whole schema there under its credentials."""
    monkeypatch.delenv("CLOUDSCALE_PG_SCHEMA", raising=False)
    result = _dlq(fresh_dsn, "list")
    assert result.returncode == 2
    assert "not a migrated cloudscale database" in result.stderr
    with psycopg.connect(fresh_dsn) as conn:
        tables = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'"
        ).fetchone()
    assert tables is not None and tables[0] == 0, "the tool created tables"
