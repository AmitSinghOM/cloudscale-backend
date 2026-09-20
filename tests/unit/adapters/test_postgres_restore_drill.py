"""The restore drill on the production tier (ADR-0016), against live PostgreSQL.

Skipped without a reachable server or a version-matched ``pg_dump`` (set
``CLOUDSCALE_PG_BIN``); CI provides both and asserts nothing skipped.
"""

from __future__ import annotations

import json
import os
import shutil
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

from cloudscale.adapters.postgres import schema  # noqa: E402
from cloudscale.application.command_service import normalize_command  # noqa: E402
from cloudscale.domain.commands import Deposit  # noqa: E402
from scripts.restore_drill import (  # noqa: E402
    DRILL_PREFIX,
    EXIT_OK,
    PostgresDrill,
    Report,
    ToolingError,
    _pg_client_major,
    _resolve_pg_bin,
    main,
)

_ADMIN_DSN = os.environ.get("CLOUDSCALE_TEST_PG", "postgresql://localhost/postgres")


def _server_major() -> int | None:
    try:
        with psycopg.connect(_ADMIN_DSN, connect_timeout=2) as conn:
            row = conn.execute(
                "SELECT current_setting('server_version_num')"
            ).fetchone()
    except psycopg.OperationalError:
        return None
    return None if row is None else int(row[0]) // 10_000


def _client_matches() -> str | None:
    server = _server_major()
    if server is None:
        return f"no PostgreSQL reachable at {_ADMIN_DSN}"
    pg_bin = _resolve_pg_bin(None)
    tool = str(pg_bin / "pg_dump") if pg_bin else shutil.which("pg_dump")
    if not tool or not Path(tool).exists():
        return "pg_dump not found (set CLOUDSCALE_PG_BIN)"
    client = _pg_client_major(tool)
    if client != server:
        return f"pg_dump is {client}, server is {server} (set CLOUDSCALE_PG_BIN)"
    return None


_SKIP = _client_matches()
pytestmark = pytest.mark.skipif(_SKIP is not None, reason=_SKIP or "")


def _drill_databases() -> set[str]:
    with psycopg.connect(_ADMIN_DSN) as conn:
        return {
            row[0]
            for row in conn.execute(
                "SELECT datname FROM pg_database WHERE datname LIKE %s",
                (f"{DRILL_PREFIX}%",),
            ).fetchall()
        }


def test_seeded_postgres_drill_passes_and_cleans_up(tmp_path: Path) -> None:
    before = _drill_databases()
    exit_code = main(
        [
            "--dsn",
            _ADMIN_DSN,
            "--seeded",
            "--report",
            str(tmp_path / "report.json"),
            "--quiet",
        ]
    )
    assert exit_code == EXIT_OK
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["pass"] is True and report["tier"] == "postgresql"
    assert report["criteria"]["schema_revision_matches_build"] == {
        "pass": True,
        "detail": f"Rev: {schema.CURRENT_REVISION} (head)",
    }
    assert report["criteria"]["adapters_start_in_migrations_mode"]["pass"] is True
    truncated = next(n for n in report["notes"] if n.startswith("truncated:"))
    assert truncated == f"truncated: {sorted(schema.DERIVED)}"
    assert report["events"] >= 20 and report["consumer"]["applied"] == report["events"]
    for phase in ("dump", "restore", "verify_revision", "full_fold", "rebuild"):
        assert report["durations_seconds"][phase] >= 0
    assert report["dump_bytes"] > 0
    # Both databases the drill created are gone.
    assert _drill_databases() == before


@pytest.fixture()
def seeded_dump(tmp_path: Path) -> Iterator[tuple[str, Path]]:
    """A seeded source database and its pg_dump; the source is dropped afterwards."""
    tier = PostgresDrill(
        _ADMIN_DSN,
        pg_bin=_resolve_pg_bin(None),
        consumer="balances",
        keep=True,  # the fixture owns the cleanup
        workdir=tmp_path,
        report=Report(tier="postgresql", mode="dump"),
    )
    with tier.environment():
        source = tier.create_database("source")
        try:
            tier.migrate_head(source)
            tier.seed(source, accounts=8)
            yield source, tier.dump(source)
        finally:
            tier._keep = False  # noqa: SLF001 - drop what the fixture created
            tier.cleanup()


def _drill_dump(dump: Path, report: Path, *, source: str | None) -> dict:
    arguments = [
        "--dsn",
        _ADMIN_DSN,
        "--dump",
        str(dump),
        "--report",
        str(report),
        "--quiet",
    ]
    if source is not None:
        arguments += ["--source-dsn", source]
    assert main(arguments) == EXIT_OK
    return json.loads(report.read_text())


def _deposit(dsn: str, account: str) -> None:
    from cloudscale.adapters.postgres.command_unit_of_work import (
        PostgresCommandUnitOfWork,
    )

    uow = PostgresCommandUnitOfWork(dsn, pool_max=1)
    try:
        result = uow.execute(
            normalize_command(
                Deposit(account, 5, 0),
                command_id=uuid.uuid4(),
                correlation_id=uuid.uuid4(),
                issuer="cloudscale",
                subject="test",
            )
        )
    finally:
        uow.close()
    assert result.outcome.value == "accepted"


def test_dump_mode_drills_an_existing_dump(
    seeded_dump: tuple[str, Path], tmp_path: Path
) -> None:
    """The annual drill's path: an existing dump instead of a seeded database."""
    source, dump = seeded_dump
    report = _drill_dump(dump, tmp_path / "dump-mode.json", source=source)
    assert report["mode"] == "dump" and report["pass"] is True
    assert report["events"] >= 20 and report["streams"] == 8


def test_dump_mode_measures_rpo_against_the_live_source(
    seeded_dump: tuple[str, Path], tmp_path: Path
) -> None:
    """rpo_events is read from the live source: 0 now, 1 after one more commit,
    and unknown (null) when no source is given — never implied."""
    source, dump = seeded_dump
    assert _drill_dump(dump, tmp_path / "first.json", source=source)["rpo_events"] == 0
    _deposit(source, "late")
    assert _drill_dump(dump, tmp_path / "second.json", source=source)["rpo_events"] == 1
    assert _drill_dump(dump, tmp_path / "third.json", source=None)["rpo_events"] is None


def test_version_mismatch_is_a_tooling_error_not_a_failed_drill(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "scripts.restore_drill._pg_client_major",
        lambda _tool: 9,  # a client nobody runs
    )
    with pytest.raises(ToolingError, match="is PostgreSQL 9 but the server is"):
        PostgresDrill(
            _ADMIN_DSN,
            pg_bin=_resolve_pg_bin(None),
            consumer="balances",
            keep=False,
            workdir=tmp_path,
            report=Report(tier="postgresql", mode="seeded"),
        )


def test_drill_leaves_the_process_environment_as_it_found_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An in-process caller (this suite) must not inherit migrations mode or a DSN.

    Regression: the first cut set ``CLOUDSCALE_PG_SCHEMA=migrations`` on the
    whole process and every later auto-mode PostgreSQL test failed with
    ``SchemaNotMigratedError``.
    """
    monkeypatch.delenv("CLOUDSCALE_PG_SCHEMA", raising=False)
    monkeypatch.setenv("CLOUDSCALE_PG_DSN", "postgresql://sentinel/keep-me")
    exit_code = main(
        [
            "--dsn",
            _ADMIN_DSN,
            "--seeded",
            "--report",
            str(tmp_path / "r.json"),
            "--quiet",
        ]
    )
    assert exit_code == EXIT_OK
    assert "CLOUDSCALE_PG_SCHEMA" not in os.environ
    assert os.environ["CLOUDSCALE_PG_DSN"] == "postgresql://sentinel/keep-me"
