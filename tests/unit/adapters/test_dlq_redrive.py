"""Redrive semantics for the dead-letter queue, plus the dlq.py CLI.

Redrive contract: re-apply + letter removal commit together (exactly-once);
the processed_events claim is kept so replays still dedupe; a redrive that
fails again re-parks the letter with an incremented attempt count.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cloudscale.adapters.sqlite_compat.dead_letter_store import (
    DeadLetteringProjectionStore,
    RedriveOutcome,
)
from scripts.dlq import EXIT_FAILED_AGAIN, EXIT_NOT_FOUND, EXIT_OK, main


def _park(
    store: DeadLetteringProjectionStore,
    event_id: str,
    log_id: int,
    event_type: str = "Deposited",
    amount: object = 25,
) -> dict:
    event = {
        "event_id": event_id,
        "id": log_id,
        "type": event_type,
        "account_id": "acct-1",
        "amount": amount,
    }
    assert store.dead_letter(event, ValueError("original failure"), attempts=3)
    return event


# -- store semantics ---------------------------------------------------------


def test_redrive_applies_the_event_and_removes_the_letter() -> None:
    store = DeadLetteringProjectionStore(path=":memory:")
    _park(store, "evt-1", 1, amount=25)

    assert store.redrive("evt-1") is RedriveOutcome.APPLIED
    assert store.dead_letter_count() == 0
    assert store.balance("acct-1")["balance"] == 25
    # Offset stays where dead-lettering put it.
    assert store.last_id() == 1


def test_redriven_event_still_dedupes_on_log_replay() -> None:
    store = DeadLetteringProjectionStore(path=":memory:")
    event = _park(store, "evt-1", 1, amount=25)
    assert store.redrive("evt-1") is RedriveOutcome.APPLIED

    # At-least-once redelivery of the same event must not double-count.
    assert store.apply(event) is False
    assert store.balance("acct-1")["balance"] == 25


def test_redrive_unknown_event_id_reports_not_found() -> None:
    store = DeadLetteringProjectionStore(path=":memory:")
    assert store.redrive("no-such-event") is RedriveOutcome.NOT_FOUND


def test_failed_redrive_reparks_with_incremented_attempts_and_fresh_error() -> None:
    store = DeadLetteringProjectionStore(path=":memory:")
    # amount=None fails typed validation on the redrive apply path.
    _park(store, "evt-bad", 1, amount=None)

    assert store.redrive("evt-bad") is RedriveOutcome.FAILED_AGAIN
    parked = store.dead_letters()
    assert len(parked) == 1
    assert parked[0]["attempts"] == 4  # 3 original + 1 failed redrive
    assert parked[0]["error_type"] != "ValueError" or (
        parked[0]["error_message"] != "original failure"
    )
    # Read model untouched by the failed redrive.
    assert store.balance("acct-1")["balance"] == 0


def test_infrastructure_failure_during_redrive_propagates_and_leaves_letter_intact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A locked/unavailable DB is not the payload's fault: no 'failed again'."""
    import sqlite3

    store = DeadLetteringProjectionStore(path=":memory:")
    _park(store, "evt-1", 1, amount=25)
    before = store.dead_letters()[0]

    def _locked(*_args: object, **_kwargs: object) -> None:
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(store, "_apply_to_balance", _locked)
    with pytest.raises(sqlite3.OperationalError):
        store.redrive("evt-1")

    after = store.dead_letters()[0]
    assert after["attempts"] == before["attempts"]  # not incremented
    assert after["error_message"] == "original failure"  # not overwritten
    assert store.balance("acct-1")["balance"] == 0


def test_redrive_all_processes_every_letter_and_reports_each_outcome() -> None:
    store = DeadLetteringProjectionStore(path=":memory:")
    _park(store, "evt-ok-1", 1, amount=10)
    _park(store, "evt-bad", 2, amount=None)
    _park(store, "evt-ok-2", 3, amount=5)

    outcomes = store.redrive_all()
    assert outcomes == {
        "evt-ok-1": RedriveOutcome.APPLIED,
        "evt-bad": RedriveOutcome.FAILED_AGAIN,
        "evt-ok-2": RedriveOutcome.APPLIED,
    }
    assert store.dead_letter_count() == 1
    assert store.balance("acct-1")["balance"] == 15


# -- CLI ----------------------------------------------------------------------


@pytest.fixture()
def parked_db(tmp_path: Path) -> str:
    db_path = str(tmp_path / "projection.db")
    store = DeadLetteringProjectionStore(path=db_path)
    _park(store, "evt-ok", 1, amount=30)
    _park(store, "evt-bad", 2, amount=None)
    store.close()
    return db_path


def test_cli_list_and_show(parked_db: str, capsys: pytest.CaptureFixture) -> None:
    assert main([parked_db, "list"]) == EXIT_OK
    output = capsys.readouterr().out
    assert "evt-ok" in output and "evt-bad" in output and "total: 2" in output

    assert main([parked_db, "show", "evt-ok"]) == EXIT_OK
    assert '"event_id": "evt-ok"' in capsys.readouterr().out

    assert main([parked_db, "show", "missing"]) == EXIT_NOT_FOUND


def test_cli_redrive_all_reports_partial_failure(
    parked_db: str, capsys: pytest.CaptureFixture
) -> None:
    assert main([parked_db, "redrive", "--all"]) == EXIT_FAILED_AGAIN
    output = capsys.readouterr().out
    assert "applied      evt-ok" in output
    assert "failed_again evt-bad" in output
    assert "still parked: 1" in output

    # The applied event is durably gone; balance is durably visible.
    store = DeadLetteringProjectionStore(path=parked_db)
    try:
        assert store.dead_letter_count() == 1
        assert store.balance("acct-1")["balance"] == 30
    finally:
        store.close()


def test_cli_redrive_single_and_argument_validation(
    parked_db: str, capsys: pytest.CaptureFixture
) -> None:
    assert main([parked_db, "redrive", "evt-ok"]) == EXIT_OK
    capsys.readouterr()
    assert main([parked_db, "redrive", "no-such"]) == EXIT_NOT_FOUND
    capsys.readouterr()
    # Neither ids nor --all, and both together, are rejected.
    assert main([parked_db, "redrive"]) == EXIT_NOT_FOUND
    capsys.readouterr()
    assert main([parked_db, "redrive", "evt-bad", "--all"]) == EXIT_NOT_FOUND


def test_cli_missing_database_is_an_error(tmp_path: Path) -> None:
    assert main([str(tmp_path / "absent.db"), "list"]) == EXIT_NOT_FOUND
