"""The restore drill on the SQLite tier (ADR-0016).

Beyond "it passes", these tests prove the drill *discriminates*: a read model
the consumer failed to rebuild, and a log row lost between dump and restore,
each turn the verdict red on the criterion that names the cause.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from cloudscale.adapters.postgres import schema
from scripts import restore_drill
from scripts.restore_drill import (
    EXIT_FAILED,
    EXIT_OK,
    Report,
    SqliteDrill,
    main,
    run_drill,
)


def _run(tier: SqliteDrill, report: Report) -> Report:
    return run_drill(
        tier,
        report,
        seeded=True,
        dump_path=None,
        accounts=8,
        source_log_tail=lambda: None,
    )


def test_seeded_sqlite_drill_passes_every_criterion(tmp_path: Path) -> None:
    exit_code = main(
        [
            "--sqlite-dir",
            str(tmp_path),
            "--seeded",
            "--report",
            str(tmp_path / "report.json"),
            "--quiet",
        ]
    )
    assert exit_code == EXIT_OK
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["pass"] is True
    assert report["tier"] == "sqlite"
    assert report["schema_revision"] == schema.CURRENT_REVISION
    failed = [name for name, c in report["criteria"].items() if not c["pass"]]
    assert failed == []
    # A pass the data could not have failed is labelled, never implied: the
    # seed has no poison events, so the dead-letter criterion is the only
    # unexercised one (independent review, ADR-0016).
    unexercised = sorted(n for n, c in report["criteria"].items() if not c["exercised"])
    assert unexercised == ["dead_letters_subset_of_dump"]
    assert (
        "not exercised" in report["criteria"]["dead_letters_subset_of_dump"]["detail"]
    )
    # Every event type the seed promises is in the log: hold lifecycle,
    # transfer, N-leg, reversal, cash. The read models it populates prove it.
    assert report["events"] >= 20 and report["streams"] == 8
    assert report["rpo_events"] == 0
    assert report["consumer"]["applied"] == report["events"]
    # The drill truncated exactly the derived tables the SQLite tier has.
    truncated = next(n for n in report["notes"] if n.startswith("truncated:"))
    for table in ("stream_snapshots", *schema.CONSUMER_REBUILT):
        assert f"'{table}'" in truncated
    # Nothing left behind by default.
    assert not [p for p in tmp_path.iterdir() if p.name.startswith("cloudscale_drill_")]


def test_drill_fails_when_a_read_model_is_not_rebuilt(tmp_path: Path) -> None:
    """Truncate, then a consumer that never runs: the fold criterion must go red."""
    report = Report(tier="sqlite", mode="seeded")
    tier = SqliteDrill(tmp_path, consumer="balances", keep=False, report=report)
    real_rebuild = tier.rebuild
    real_seed = tier.seed

    def seed_then_break_the_consumer(dsn: str, *, accounts: int) -> dict[str, int]:
        counts = real_seed(dsn, accounts=accounts)
        tier.rebuild = lambda _dsn: {  # type: ignore[method-assign]
            "applied": 0,
            "dead_lettered": 0,
            "duplicates": 0,
            "halted": False,
            "halt_reason": None,
        }
        return counts

    tier.seed = seed_then_break_the_consumer  # type: ignore[method-assign]
    _run(tier, report)
    assert real_rebuild is not tier.rebuild
    assert report.passed is False
    assert report.criteria["rebuilt_balances_equal_full_fold"]["pass"] is False
    assert report.criteria["rebuilt_balances_equal_dump"]["pass"] is False
    assert report.criteria["processed_events_equal_events"]["pass"] is False
    # The log itself was fine, so the fold-only facts still hold.
    assert report.criteria["schema_revision_matches_build"]["pass"] is True


def test_drill_detects_a_log_row_lost_between_dump_and_restore(tmp_path: Path) -> None:
    """A restore that silently drops the last event: rebuilt balances no longer
    match the read models the dump carried, and the drill says so."""
    report = Report(tier="sqlite", mode="seeded")
    tier = SqliteDrill(tmp_path, consumer="balances", keep=False, report=report)
    real_restore = tier.restore

    def restore_and_lose_a_row(dump_path: Path) -> str:
        dsn = real_restore(dump_path)
        conn = sqlite3.connect(str(Path(dsn) / "log.db"))
        try:
            conn.execute("DELETE FROM events WHERE id = (SELECT MAX(id) FROM events)")
            conn.commit()
        finally:
            conn.close()
        return dsn

    tier.restore = restore_and_lose_a_row  # type: ignore[method-assign]
    _run(tier, report)
    assert report.passed is False
    assert report.criteria["rebuilt_balances_equal_full_fold"]["pass"] is True
    assert report.criteria["rebuilt_balances_equal_dump"]["pass"] is False


def test_main_exits_3_when_a_criterion_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def wrong_fold(self: SqliteDrill, dsn: str, streams: dict[str, str]) -> dict:
        return {account: (0, 0, 0) for account in streams.values()}

    monkeypatch.setattr(restore_drill.SqliteDrill, "fold_all", wrong_fold)
    exit_code = main(
        [
            "--sqlite-dir",
            str(tmp_path),
            "--seeded",
            "--report",
            str(tmp_path / "r.json"),
            "--quiet",
        ]
    )
    assert exit_code == EXIT_FAILED
    report = json.loads((tmp_path / "r.json").read_text())
    assert report["pass"] is False
    assert report["criteria"]["rebuilt_balances_equal_full_fold"]["pass"] is False
    assert report["criteria"]["rebuilt_balances_equal_dump"]["pass"] is True


def test_keep_leaves_the_databases_for_inspection(tmp_path: Path) -> None:
    exit_code = main(
        [
            "--sqlite-dir",
            str(tmp_path),
            "--seeded",
            "--report",
            str(tmp_path / "r.json"),
            "--keep",
            "--quiet",
        ]
    )
    assert exit_code == EXIT_OK
    kept = sorted(
        p.name for p in tmp_path.iterdir() if p.name.startswith("cloudscale_drill_")
    )
    assert any(n.startswith("cloudscale_drill_source_") for n in kept)
    assert any(n.startswith("cloudscale_drill_restored_") for n in kept)


def test_open_holds_from_rows_is_placed_minus_closed() -> None:
    rows = [
        ("HoldPlaced", "h1"),
        ("HoldPlaced", "h2"),
        ("HoldPosted", "h1"),
        ("HoldPlaced", "h3"),
        ("HoldReleased", "h3"),
    ]
    assert restore_drill._open_holds_from_rows(rows) == frozenset({"h2"})


def test_seed_refuses_too_few_accounts(tmp_path: Path) -> None:
    exit_code = main(
        [
            "--sqlite-dir",
            str(tmp_path),
            "--seeded",
            "--accounts",
            "3",
            "--report",
            str(tmp_path / "r.json"),
            "--quiet",
        ]
    )
    assert exit_code == restore_drill.EXIT_TOOLING
