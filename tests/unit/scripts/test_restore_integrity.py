"""Backup integrity is independent of folding the already-restored event log."""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from scripts.restore_drill import (
    EXIT_FAILED,
    PostgresDrill,
    Report,
    SqliteDrill,
    _resolve_pg_bin,
    main,
    run_drill,
)


@pytest.fixture(params=["sqlite", "postgresql"])
def tier(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator:
    report = Report(tier=request.param, mode="seeded")
    if request.param == "sqlite":
        drill = SqliteDrill(tmp_path, consumer="balances", keep=False, report=report)
    else:
        dsn = os.environ.get("CLOUDSCALE_TEST_PG")
        if dsn is None:
            pytest.skip("CLOUDSCALE_TEST_PG required for isolated PostgreSQL tests")
        drill = PostgresDrill(
            dsn,
            pg_bin=_resolve_pg_bin(None),
            consumer="balances",
            keep=False,
            workdir=tmp_path,
            report=report,
        )
    try:
        yield drill, report
    finally:
        drill.cleanup()


def _mutate(drill, dsn: str, sql: str) -> None:
    if isinstance(drill, SqliteDrill):
        conn = sqlite3.connect(str(Path(dsn) / "log.db"))
    else:
        import psycopg

        conn = psycopg.connect(dsn)
    try:
        conn.execute(sql)
        conn.commit()
    finally:
        conn.close()


def _run(drill, report: Report) -> Report:
    return run_drill(
        drill,
        report,
        seeded=True,
        dump_path=None,
        accounts=8,
        source_log_tail=lambda: None,
    )


@pytest.mark.parametrize(
    ("table", "sql"),
    [
        ("accounts", "DELETE FROM accounts"),
        (
            "command_results",
            "DELETE FROM command_results WHERE result_json LIKE '%accepted%'",
        ),
        (
            "command_results",
            "DELETE FROM command_results WHERE result_json NOT LIKE '%accepted%'",
        ),
        ("event_envelopes", "DELETE FROM event_envelopes"),
        ("accounts", "UPDATE accounts SET owner_subject = 'changed-owner'"),
        ("command_results", "UPDATE command_results SET request_hash = NULL"),
        (
            "command_results",
            "UPDATE command_results SET result_json = replace(result_json, 'accepted', 'version_conflict')",
        ),
        ("events", "UPDATE events SET amount = amount + 1 WHERE id = 1"),
        (
            "event_envelopes",
            "UPDATE event_envelopes SET correlation_id = 'changed-correlation'",
        ),
    ],
)
def test_restore_detects_missing_or_changed_authoritative_content(
    tier, table: str, sql: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    drill, report = tier
    real_restore = drill.restore

    def corrupt_restored_copy(dump: Path) -> str:
        restored = real_restore(dump)
        mutation = sql
        if "request_hash = NULL" in mutation:
            value = "X'00'" if isinstance(drill, SqliteDrill) else "decode('00', 'hex')"
            mutation = mutation.replace("NULL", value)
        _mutate(drill, restored, mutation)
        return restored

    monkeypatch.setattr(drill, "restore", corrupt_restored_copy)
    _run(drill, report)
    assert report.passed is False
    criterion = report.criteria[f"backup_{table}_preserved"]
    assert criterion["pass"] is False and criterion["exercised"] is True
    assert "changed-owner" not in report.to_json()
    assert "changed-correlation" not in report.to_json()


def test_rebuild_detects_authoritative_content_mutation(tier, monkeypatch) -> None:
    drill, report = tier
    real_truncate = drill.truncate_derived

    def truncate_and_corrupt(dsn: str) -> list[str]:
        tables = real_truncate(dsn)
        _mutate(drill, dsn, "DELETE FROM command_results")
        return tables

    monkeypatch.setattr(drill, "truncate_derived", truncate_and_corrupt)
    _run(drill, report)
    assert report.passed is False
    assert report.criteria["backup_command_results_preserved"]["pass"] is True
    assert report.criteria["rebuild_command_results_preserved"]["pass"] is False


def test_dump_without_independent_baseline_does_not_claim_backup_integrity(
    tier,
) -> None:
    drill, report = tier
    report.mode = "dump"
    with drill.environment():
        source = drill.create_database("source")
        drill.migrate_head(source)
        drill.seed(source, accounts=8)
        dump = drill.dump(source)
    run_drill(
        drill,
        report,
        seeded=False,
        dump_path=dump,
        accounts=8,
        source_log_tail=lambda: None,
    )
    assert report.passed is True
    for table in ("accounts", "command_results", "events", "event_envelopes"):
        criterion = report.criteria[f"backup_{table}_preserved"]
        assert criterion["exercised"] is False
        assert "not exercised" in criterion["detail"]
        assert report.criteria[f"rebuild_{table}_preserved"]["exercised"] is True


def test_cli_exits_3_on_ownership_loss(tmp_path: Path, monkeypatch) -> None:
    real_restore = SqliteDrill.restore

    def corrupt(self, dump: Path) -> str:
        restored = real_restore(self, dump)
        _mutate(self, restored, "DELETE FROM accounts")
        return restored

    monkeypatch.setattr(SqliteDrill, "restore", corrupt)
    report = tmp_path / "report.json"
    assert (
        main(
            [
                "--sqlite-dir",
                str(tmp_path),
                "--seeded",
                "--quiet",
                "--report",
                str(report),
            ]
        )
        == EXIT_FAILED
    )
    assert (
        json.loads(report.read_text())["criteria"]["backup_accounts_preserved"]["pass"]
        is False
    )


@pytest.mark.parametrize("tier", ["postgresql"], indirect=True)
def test_published_flag_is_the_only_excluded_event_content(tier, monkeypatch) -> None:
    drill, report = tier
    real_restore = drill.restore

    def reset_publication(dump: Path) -> str:
        restored = real_restore(dump)
        _mutate(drill, restored, "UPDATE events SET published = NOT published")
        return restored

    monkeypatch.setattr(drill, "restore", reset_publication)
    _run(drill, report)
    assert report.passed is True
    assert report.criteria["backup_events_preserved"]["exercised"] is True
    assert report.criteria["rebuild_events_preserved"]["exercised"] is True


def test_empty_authoritative_comparison_is_marked_unexercised(tier) -> None:
    from scripts.restore_drill import _compare_authoritative

    drill, report = tier
    with drill.environment():
        empty = drill.create_database("empty")
        drill.migrate_head(empty)
        if isinstance(drill, SqliteDrill):
            from cloudscale.adapters.sqlite_compat.account_registry import (
                SqliteAccountRegistry,
            )

            SqliteAccountRegistry(str(Path(empty) / "log.db")).close()
        snapshot = drill.authoritative_snapshot(empty)
    _compare_authoritative(report, "backup", snapshot, snapshot)
    assert report.passed is True
    assert all(not criterion["exercised"] for criterion in report.criteria.values())


def test_empty_schema_mismatch_is_an_exercised_failure() -> None:
    from scripts.restore_drill import TableFingerprint, _compare_authoritative
    from cloudscale.adapters.postgres import schema

    report = Report(tier="sqlite", mode="seeded")
    before = {table: TableFingerprint(0, "before") for table in schema.SYSTEM_OF_RECORD}
    after = {**before, "accounts": TableFingerprint(0, "changed-columns")}
    _compare_authoritative(report, "backup", before, after)
    assert report.passed is False
    assert report.criteria["backup_accounts_preserved"]["exercised"] is True
    assert report.criteria["backup_command_results_preserved"]["exercised"] is False
