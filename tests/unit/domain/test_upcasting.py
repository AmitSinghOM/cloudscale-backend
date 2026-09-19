"""ADR-0009: every historical event shape upcasts and folds; unknown shapes fail loudly."""

from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

import pytest

from cloudscale.adapters.sqlite_compat.dead_letter_store import (
    DeadLetteringProjectionStore,
)
from cloudscale.domain import upcasting
from cloudscale.domain.upcasting import (
    CURRENT_SCHEMA_VERSION,
    UnknownSchemaVersionError,
    register_upcaster,
    upcast,
)
from cloudscale.processes.resilient_consumer import ResilientConsumer
from cloudscale.domain.events import BALANCE_SIGN, HELD_SIGN
from cqrs import BalanceProjection, SqliteEventStore

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "events"


def _fixtures() -> list[tuple[str, int, dict]]:
    out = []
    for path in sorted(FIXTURES.glob("*.v*.json")):
        event_type, version = path.stem.split(".v")
        out.append((event_type, int(version), json.loads(path.read_text())))
    return out


@pytest.fixture()
def clean_registry(monkeypatch):
    """Isolate registrations made by a test."""
    monkeypatch.setattr(upcasting, "_REGISTRY", {})
    yield


# -- corpus completeness and foldability --------------------------------------------


def test_every_version_ever_written_has_a_fixture() -> None:
    present = {(t, v) for t, v, _ in _fixtures()}
    expected = {
        (event_type, version)
        for event_type, current in CURRENT_SCHEMA_VERSION.items()
        for version in range(1, current + 1)
    }
    assert present == expected, (
        f"missing={expected - present} extra={present - expected}"
    )


@pytest.mark.parametrize("event_type,version,row", _fixtures())
def test_every_fixture_upcasts_and_folds_through_the_projection(
    event_type: str, version: int, row: dict
) -> None:
    current = upcast(row)
    assert current["type"] == event_type
    assert current["schema_version"] == CURRENT_SCHEMA_VERSION[event_type]
    assert row["schema_version"] == version  # fixture names are honest
    projection = BalanceProjection()
    state = projection.apply(projection.initial(), current)
    assert state["account_id"] == row["account_id"]
    # Each type moves exactly the quantities its sign tables say (ADR-0014).
    assert state["balance"] == BALANCE_SIGN[event_type] * row["amount"]
    assert state["held"] == HELD_SIGN[event_type] * row["amount"]
    # Identity fields pass through untouched.
    for key in ("id", "event_id", "stream", "seq"):
        assert current[key] == row[key]


# -- upcast semantics -----------------------------------------------------------------


def test_missing_schema_version_reads_as_v1_and_is_stamped() -> None:
    legacy = {"type": "Deposited", "account_id": "a", "amount": 5}
    assert upcast(legacy)["schema_version"] == CURRENT_SCHEMA_VERSION["Deposited"]


@pytest.mark.parametrize("bad", [0, -1, "1", None, 1.0])
def test_invalid_schema_version_is_rejected(bad) -> None:
    with pytest.raises(UnknownSchemaVersionError, match="invalid schema_version"):
        upcast(
            {"type": "Deposited", "account_id": "a", "amount": 5, "schema_version": bad}
        )


def test_future_version_is_rejected_not_guessed() -> None:
    row = {"type": "Deposited", "account_id": "a", "amount": 5, "schema_version": 99}
    with pytest.raises(UnknownSchemaVersionError, match="newer than this build"):
        upcast(row)


def test_unknown_event_type_is_rejected() -> None:
    with pytest.raises(UnknownSchemaVersionError, match="unknown event type"):
        upcast({"type": "Teleported", "schema_version": 1})


def test_steps_chain_one_version_at_a_time(clean_registry, monkeypatch) -> None:
    """Simulate a type at v3 with two registered steps; a v1 row walks 1→2→3."""
    monkeypatch.setitem(CURRENT_SCHEMA_VERSION, "Deposited", 3)

    @register_upcaster("Deposited", 1)
    def v1_to_v2(event: dict) -> dict:
        renamed = {k: v for k, v in event.items() if k != "amount"}
        return {**renamed, "amount_minor": event["amount"]}

    @register_upcaster("Deposited", 2)
    def v2_to_v3(event: dict) -> dict:
        return {**event, "currency": "USD"}

    out = upcast(
        {"type": "Deposited", "account_id": "a", "amount": 7, "schema_version": 1}
    )
    assert out == {
        "type": "Deposited",
        "account_id": "a",
        "amount_minor": 7,
        "currency": "USD",
        "schema_version": 3,
    }
    # A v2 row only takes the second step.
    mid = upcast(
        {"type": "Deposited", "account_id": "a", "amount_minor": 1, "schema_version": 2}
    )
    assert mid["currency"] == "USD" and "amount" not in mid


def test_missing_step_in_the_chain_is_an_error(clean_registry, monkeypatch) -> None:
    monkeypatch.setitem(CURRENT_SCHEMA_VERSION, "Deposited", 2)
    with pytest.raises(UnknownSchemaVersionError, match="no upcaster registered"):
        upcast(
            {"type": "Deposited", "account_id": "a", "amount": 1, "schema_version": 1}
        )


def test_registry_rejects_out_of_range_and_duplicate_steps(
    clean_registry, monkeypatch
) -> None:
    with pytest.raises(ValueError, match="outside"):
        register_upcaster("Deposited", 1)  # current is 1: nothing to step from
    monkeypatch.setitem(CURRENT_SCHEMA_VERSION, "Deposited", 2)
    register_upcaster("Deposited", 1)(lambda e: e)
    with pytest.raises(ValueError, match="already registered"):
        register_upcaster("Deposited", 1)(lambda e: e)
    with pytest.raises(ValueError, match="unknown event type"):
        register_upcaster("Teleported", 1)


# -- integration: the read boundaries ----------------------------------------------------


def test_consumer_dead_letters_an_event_from_the_future_without_wedging(
    tmp_path,
) -> None:
    """A v99 row (written by a newer build) must park, not halt; neighbours apply."""
    store = SqliteEventStore(str(tmp_path / "log.db"))
    projection = DeadLetteringProjectionStore(path=str(tmp_path / "p.db"))
    try:
        base = {"type": "Deposited", "account_id": "acct", "amount": 10}
        store.append("account-acct", {**base, "event_id": str(uuid.uuid4())})
        store.append(
            "account-acct", {**base, "event_id": "future-1", "schema_version": 99}
        )
        store.append("account-acct", {**base, "event_id": str(uuid.uuid4())})

        report = ResilientConsumer(store, projection).run()
        assert report.applied == 2
        assert report.dead_lettered == 1
        assert report.halted is False
        parked = projection.dead_letters()
        assert [p["event_id"] for p in parked] == ["future-1"]
        assert parked[0]["error_type"] == "UnknownSchemaVersionError"
        assert projection.balance("acct")["balance"] == 20
    finally:
        projection.close()
        store.close()


def test_legacy_log_without_schema_version_column_is_upgraded_and_reads_as_v1(
    tmp_path,
) -> None:
    path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(path)
    conn.executescript(
        "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL,"
        " stream TEXT NOT NULL, seq INTEGER NOT NULL, type TEXT NOT NULL,"
        " account_id TEXT, amount INTEGER, UNIQUE (stream, seq), UNIQUE (event_id));"
        "INSERT INTO events (event_id, stream, seq, type, account_id, amount)"
        " VALUES ('old-1', 'account-x', 1, 'Deposited', 'x', 42);"
    )
    conn.commit()
    conn.close()

    store = SqliteEventStore(path)
    try:
        [event] = store.read("account-x")
        assert event["schema_version"] == 1
        assert upcast(event)["amount"] == 42
    finally:
        store.close()


# -- writer/reader contract ---------------------------------------------------------


def test_writer_stamps_the_current_schema_version_not_a_literal(
    tmp_path, clean_registry, monkeypatch
) -> None:
    """The stamp a writer puts on a row must be what readers expect to upcast FROM.

    If the writer hardcodes 1 while CURRENT_SCHEMA_VERSION says 2, every reader
    applies the 1->2 step to a payload that is already v2 -- silent corruption of
    the source of record on the first real evolution. Bump the version the way
    ``upcasting.py`` instructs and check the raw row.
    """
    from uuid import uuid4

    from cloudscale.adapters.sqlite_compat.command_unit_of_work import (
        SqliteCommandUnitOfWork,
    )
    from cloudscale.application.command_service import normalize_command
    from cloudscale.domain.commands import Deposit

    monkeypatch.setitem(CURRENT_SCHEMA_VERSION, "Deposited", 2)
    register_upcaster("Deposited", 1)(lambda e: dict(e))  # identity step 1 -> 2

    path = str(tmp_path / "stamp.db")
    uow = SqliteCommandUnitOfWork(path)
    try:
        uow.execute(
            normalize_command(
                Deposit(account_id="acct-stamp", amount=5, expected_version=0),
                command_id=uuid4(),
                correlation_id=uuid4(),
                issuer="cloudscale",
                subject="user-1",
            )
        )
    finally:
        uow.close()

    conn = sqlite3.connect(path)
    try:
        [(stored,)] = conn.execute("SELECT schema_version FROM events").fetchall()
    finally:
        conn.close()
    assert stored == CURRENT_SCHEMA_VERSION["Deposited"], (
        f"writer stamped v{stored} but this build's current shape is "
        f"v{CURRENT_SCHEMA_VERSION['Deposited']}"
    )
