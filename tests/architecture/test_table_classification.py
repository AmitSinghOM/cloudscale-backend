"""Every table is classified, and the runbook agrees with the code (ADR-0016).

The classification in ``schema.py`` is what a backup must contain and what a
restore may discard. Three things may not drift from it:

- the three classes partition the tables (no overlap, nothing unclassified);
- every ``CREATE TABLE`` in the SQLite tier's DDL names a classified table
  (the PostgreSQL side is checked against ``information_schema`` in
  ``tests/unit/adapters/test_postgres_migrations.py``);
- RUNBOOK R7 names every table in each class and no other, so the list an
  operator reads during a restore is the list the code enforces.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from cloudscale.adapters.postgres import schema

ROOT = Path(__file__).resolve().parents[2]
CREATE_TABLE = re.compile(r"CREATE TABLE IF NOT EXISTS\s+(\w+)")

#: The SQLite tier defines its tables in these modules (log side, then
#: projection side). A new module that creates tables must be listed here or
#: the completeness test cannot see it.
SQLITE_DDL_MODULES = (
    "cloudscale/adapters/sqlite_compat/command_unit_of_work.py",
    "cloudscale/adapters/sqlite_compat/account_registry.py",
    "cloudscale/adapters/sqlite_compat/dead_letter_store.py",
    "cqrs/idempotent_consumer.py",
    "cqrs/durable_eventstore.py",
)


def _all_classified() -> frozenset[str]:
    return schema.SYSTEM_OF_RECORD | schema.DERIVED | schema.EPHEMERAL | schema.TOOLING


def test_classes_are_disjoint() -> None:
    classes = (
        schema.SYSTEM_OF_RECORD,
        schema.DERIVED,
        schema.EPHEMERAL,
        schema.TOOLING,
    )
    for index, left in enumerate(classes):
        for right in classes[index + 1 :]:
            assert not (left & right), f"table in two classes: {left & right}"


def test_every_table_in_the_postgres_ddl_is_classified() -> None:
    declared = set(CREATE_TABLE.findall("".join(schema.ALL)))
    assert declared, "no CREATE TABLE statements found in schema.ALL"
    unclassified = declared - _all_classified()
    assert not unclassified, f"unclassified tables (ADR-0016): {sorted(unclassified)}"
    # And nothing classified is a phantom: every class member exists.
    phantom = (schema.SYSTEM_OF_RECORD | schema.DERIVED | schema.EPHEMERAL) - declared
    assert not phantom, f"classified but not declared: {sorted(phantom)}"


def test_every_table_in_the_sqlite_ddl_is_classified() -> None:
    declared: set[str] = set()
    for relative in SQLITE_DDL_MODULES:
        declared |= set(CREATE_TABLE.findall((ROOT / relative).read_text()))
    assert declared
    unclassified = declared - _all_classified()
    assert not unclassified, (
        f"unclassified SQLite tables (ADR-0016): {sorted(unclassified)}"
    )


def test_consumer_rebuilt_is_a_subset_of_derived() -> None:
    assert set(schema.CONSUMER_REBUILT) <= schema.DERIVED
    assert len(set(schema.CONSUMER_REBUILT)) == len(schema.CONSUMER_REBUILT)


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        ("events", "system_of_record"),
        ("accounts", "system_of_record"),
        ("balances", "derived"),
        ("stream_snapshots", "derived"),
        ("rate_limit_buckets", "ephemeral"),
        ("alembic_version", "tooling"),
    ],
)
def test_classify(table: str, expected: str) -> None:
    assert schema.classify(table) == expected


def test_classify_refuses_unknown_table() -> None:
    with pytest.raises(KeyError, match="not classified"):
        schema.classify("audit_trail")


# -- RUNBOOK R7 must list exactly what the code classifies -----------------------

R7_HEADING = re.compile(r"^## R7 ", re.MULTILINE)
NEXT_HEADING = re.compile(r"^## ", re.MULTILINE)
CLASS_BLOCK = re.compile(
    r"\*\*(System of record|Derived|Ephemeral)\*\*[^\n]*\n((?:[^\n]*\n)*?)\n",
)
BACKTICKED = re.compile(r"`([a-z_]+)`")


def _r7_section() -> str:
    text = (ROOT / "docs" / "RUNBOOK.md").read_text()
    start = R7_HEADING.search(text)
    assert start, "RUNBOOK has no R7 section"
    rest = text[start.end() :]
    end = NEXT_HEADING.search(rest)
    return rest[: end.start()] if end else rest


def _r7_classes() -> dict[str, set[str]]:
    section = _r7_section()
    found: dict[str, set[str]] = {}
    for match in CLASS_BLOCK.finditer(section):
        label = match.group(1)
        # Table names are the backticked identifiers in the block, minus
        # column references written as ``table.column``.
        block = re.sub(r"`[a-z_]+\.[a-z_]+`", "", match.group(2))
        found[label] = set(BACKTICKED.findall(block))
    return found


def test_runbook_r7_lists_exactly_the_classified_tables() -> None:
    classes = _r7_classes()
    assert set(classes) == {"System of record", "Derived", "Ephemeral"}, (
        f"R7 must have one **System of record**, **Derived**, **Ephemeral** block; found {sorted(classes)}"
    )
    assert classes["System of record"] == set(schema.SYSTEM_OF_RECORD), classes[
        "System of record"
    ] ^ set(schema.SYSTEM_OF_RECORD)
    assert classes["Derived"] == set(schema.DERIVED), classes["Derived"] ^ set(
        schema.DERIVED
    )
    assert classes["Ephemeral"] == set(schema.EPHEMERAL), classes["Ephemeral"] ^ set(
        schema.EPHEMERAL
    )
