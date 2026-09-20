#!/usr/bin/env python3
"""Restore drill (ADR-0016): prove the derived tables are derivable.

The event log is the sole source of record (ADR-0002). This script turns that
claim into evidence: dump a database, restore it into a fresh one, confirm
the schema revision, start the adapters in ``migrations`` mode, **truncate
every table classified DERIVED**, let the consumer rebuild them, and compare
the result against a full fold of the restored log *and* against the read
models the dump carried. Pass criteria are fixed in code; the report goes to
``evidence/<git-sha>/restore-drill/report.json``.

Usage::

    # CI / every release: seed a throwaway database with every event type, then drill it
    python scripts/restore_drill.py --dsn postgresql://user:pw@host/postgres --seeded
    python scripts/restore_drill.py --sqlite-dir /tmp/drill --seeded

    # Annual, production scale: drill an existing pg_dump (custom format)
    python scripts/restore_drill.py --dsn postgresql://user:pw@host/postgres --dump backup.dump \\
        [--source-dsn postgresql://.../live]   # lets the report measure rpo_events honestly

``--dsn`` is an administrative connection that may ``CREATE DATABASE``; every
database this script creates is named ``cloudscale_drill_*`` and dropped
afterwards unless ``--keep``. ``pg_dump``/``pg_restore`` are taken from
``--pg-bin``, ``CLOUDSCALE_PG_BIN`` or ``PATH`` and must match the server's
major version.

Exit codes: 0 every criterion passed; 3 at least one comparison failed (the
report says which); 2 tooling — client/server version mismatch, missing
binaries, an unreachable server, a dump that will not restore.

On PostgreSQL the adapters run in ``migrations`` mode for the whole drill
(ADR-0007): they verify the restored revision and never create schema.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
import sys
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from cloudscale.adapters.postgres import schema  # noqa: E402
from cloudscale.application.command_service import normalize_command  # noqa: E402
from cloudscale.domain.commands import (  # noqa: E402
    Deposit,
    ExpireHold,
    Hold,
    Leg,
    Post,
    PostHold,
    Revert,
    Transfer,
    VoidHold,
    Withdraw,
)
from cloudscale.domain.results import CommandOutcome  # noqa: E402
from cloudscale.processes.resilient_consumer import ResilientConsumer  # noqa: E402

EXIT_OK = 0
EXIT_TOOLING = 2
EXIT_FAILED = 3

DRILL_PREFIX = "cloudscale_drill_"
HOLD_EVENT_TYPES = ("HoldPlaced", "HoldPosted", "HoldReleased")
CONSUMER_BATCH = 500


class ToolingError(RuntimeError):
    """The drill could not run; this is not a restore failure."""


@contextmanager
def scoped_environment(**values: str) -> Iterator[None]:
    """Set environment variables for the duration of the block, then restore.

    The adapters read ``CLOUDSCALE_PG_SCHEMA`` and Alembic reads
    ``CLOUDSCALE_PG_DSN`` from the process environment; an in-process caller
    (the test suite) must not inherit the drill's settings after it returns.
    """
    previous = {name: os.environ.get(name) for name in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


# -- report -------------------------------------------------------------------------


@dataclass
class Report:
    tier: str
    mode: str
    schema_revision: str = schema.CURRENT_REVISION
    git_sha: str | None = None
    generated_at: str = field(
        default_factory=lambda: datetime.now(UTC).isoformat(timespec="seconds")
    )
    events: int = 0
    streams: int = 0
    dump_bytes: int = 0
    rpo_events: int | None = None
    durations_seconds: dict[str, float] = field(default_factory=dict)
    rebuild_events_per_second: float | None = None
    consumer: dict[str, int | bool | str | None] = field(default_factory=dict)
    criteria: dict[str, dict[str, Any]] = field(default_factory=dict)
    classification: dict[str, list[str]] = field(
        default_factory=lambda: {
            "system_of_record": sorted(schema.SYSTEM_OF_RECORD),
            "derived": sorted(schema.DERIVED),
            "ephemeral": sorted(schema.EPHEMERAL),
        }
    )
    notes: list[str] = field(default_factory=list)

    def criterion(self, name: str, passed: bool, detail: str) -> None:
        self.criteria[name] = {"pass": bool(passed), "detail": detail}

    @property
    def passed(self) -> bool:
        return bool(self.criteria) and all(c["pass"] for c in self.criteria.values())

    def to_json(self) -> str:
        payload = {**self.__dict__, "pass": self.passed}
        return json.dumps(payload, indent=2, sort_keys=True)


def _timed(report: Report, name: str, action: Callable[[], Any]) -> Any:
    started = time.perf_counter()
    try:
        return action()
    finally:
        report.durations_seconds[name] = round(time.perf_counter() - started, 4)


def _git_sha() -> str | None:
    try:
        completed = subprocess.run(  # noqa: S603 - fixed argv, no user input
            ["git", "rev-parse", "HEAD"],
            cwd=REPOSITORY_ROOT,
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return completed.stdout.strip() or None


# -- what a tier must offer the drill -----------------------------------------------


@dataclass(frozen=True)
class ReadModelSnapshot:
    """The derived tables as rows, comparable across truncate-and-rebuild."""

    balances: dict[str, tuple[int, int, int]]  # account_id -> (balance, held, version)
    holds: frozenset[tuple[Any, ...]]
    transfers: frozenset[tuple[Any, ...]]
    transfer_legs: frozenset[tuple[Any, ...]]
    dead_letter_ids: frozenset[str]
    processed_events: int
    open_hold_ids: frozenset[str]

    @property
    def total_balance(self) -> int:
        return sum(balance for balance, _held, _version in self.balances.values())


@dataclass(frozen=True)
class LogFacts:
    """Facts read from the source-of-record tables only."""

    events: int
    tail_id: int
    streams: dict[str, str]  # stream -> account_id
    open_hold_ids: frozenset[str]


def _open_holds_from_rows(rows: Iterable[tuple[str, str]]) -> frozenset[str]:
    """``hold_id`` of every ``HoldPlaced`` without a later ``HoldPosted``/``HoldReleased``."""
    placed: set[str] = set()
    closed: set[str] = set()
    for event_type, hold_id in rows:
        if event_type == "HoldPlaced":
            placed.add(hold_id)
        else:
            closed.add(hold_id)
    return frozenset(placed - closed)


# -- seed workload (shared by both tiers) -------------------------------------------


class SeedError(RuntimeError):
    pass


def _request(command: Any, *, command_id: uuid.UUID | None = None) -> Any:
    return normalize_command(
        command,
        command_id=command_id or uuid.uuid4(),
        correlation_id=uuid.uuid4(),
        issuer="cloudscale",
        subject="restore-drill",
    )


def _accepted(uow: Any, command: Any, *, command_id: uuid.UUID | None = None) -> Any:
    result = uow.execute(_request(command, command_id=command_id))
    if result.outcome is not CommandOutcome.ACCEPTED:
        raise SeedError(f"seed command rejected: {command!r} -> {result.outcome}")
    return result


def _version(uow: Any, account: str) -> int:
    return int(uow.fold_stream(account).version)


def seed_workload(
    uow: Any,
    future_uow: Any,
    registry: Any,
    drain: Callable[[], object],
    *,
    accounts: int,
) -> dict[str, int]:
    """Every event type in the fixture corpus, every read model populated.

    ``future_uow`` shares the database but reads a clock two hours ahead, so
    ``ExpireHold`` can be issued exactly as the sweeper would after the TTL.
    Returns counts for the report.
    """
    if accounts < 8:
        raise SeedError("the seed needs at least 8 accounts")
    ids = [f"drill{index:02d}" for index in range(accounts)]
    counts = {"accepted": 0, "rejected": 0}

    for account in ids:
        registry.register(account, owner_subject=f"owner-{account}")
        _accepted(uow, Deposit(account, 1_000, 0))
        counts["accepted"] += 1

    a, b, c, d, e, f, g, h = ids[:8]

    transfer_id = uuid.uuid4()
    _accepted(uow, Transfer(a, b, 100, _version(uow, a)), command_id=transfer_id)
    _accepted(uow, Transfer(b, c, 50, _version(uow, b)))
    _accepted(
        uow,
        Post(
            a,
            (Leg(a, 60, "debit"), Leg(b, 30, "credit"), Leg(c, 30, "credit")),
            _version(uow, a),
        ),
    )
    _accepted(uow, Withdraw(d, 10, _version(uow, d)))
    counts["accepted"] += 4

    # A persisted rejection: command_results carries it, the log does not.
    rejected = uow.execute(_request(Withdraw(d, 10**9, _version(uow, d))))
    if rejected.outcome is CommandOutcome.ACCEPTED:
        raise SeedError("the overdraft was accepted; the seed is wrong")
    counts["rejected"] += 1

    posted_hold = uuid.uuid4()
    _accepted(uow, Hold(e, f, 200, _version(uow, e), 3_600), command_id=posted_hold)
    _accepted(uow, PostHold(e, posted_hold, _version(uow, e), amount=120))  # partial
    voided_hold = uuid.uuid4()
    _accepted(uow, Hold(e, g, 100, _version(uow, e), 3_600), command_id=voided_hold)
    _accepted(uow, VoidHold(e, voided_hold, _version(uow, e)))
    expired_hold = uuid.uuid4()
    _accepted(uow, Hold(f, a, 40, _version(uow, f), 1), command_id=expired_hold)
    open_hold = uuid.uuid4()
    _accepted(uow, Hold(g, h, 25, _version(uow, g), 3_600), command_id=open_hold)
    counts["accepted"] += 6

    _accepted(uow, Revert(a, transfer_id, _version(uow, a)))
    counts["accepted"] += 1

    drain()
    _accepted(future_uow, ExpireHold(f, expired_hold, _version(uow, f)))
    counts["accepted"] += 1
    drain()
    return counts


# -- PostgreSQL tier ----------------------------------------------------------------


def _is_postgres(target: str) -> bool:
    return target.startswith(("postgresql://", "postgres://"))


def _database_of(dsn: str) -> str:
    return dsn.rsplit("/", 1)[1].split("?", 1)[0]


def _with_database(admin_dsn: str, database: str) -> str:
    base, _admin_db = admin_dsn.rsplit("/", 1)
    return f"{base}/{database}"


def _resolve_pg_bin(explicit: str | None) -> Path | None:
    candidate = explicit or os.environ.get("CLOUDSCALE_PG_BIN")
    return Path(candidate) if candidate else None


def _pg_tool(pg_bin: Path | None, name: str) -> str:
    if pg_bin is not None:
        tool = pg_bin / name
        if not tool.exists():
            raise ToolingError(f"{tool} not found")
        return str(tool)
    found = shutil.which(name)
    if found is None:
        raise ToolingError(f"{name} not on PATH (set --pg-bin or CLOUDSCALE_PG_BIN)")
    return found


def _pg_client_major(tool: str) -> int:
    completed = subprocess.run(  # noqa: S603 - fixed argv
        [tool, "--version"], capture_output=True, text=True, check=True, timeout=30
    )
    # "pg_dump (PostgreSQL) 17.10 (Homebrew)" -> 17
    for token in completed.stdout.split():
        if token[0].isdigit():
            return int(token.split(".")[0])
    raise ToolingError(f"cannot parse version from {completed.stdout!r}")


class PostgresDrill:
    def __init__(
        self,
        admin_dsn: str,
        *,
        pg_bin: Path | None,
        consumer: str,
        keep: bool,
        workdir: Path,
        report: Report,
    ) -> None:
        import psycopg

        self._psycopg = psycopg
        self._admin_dsn = admin_dsn
        self._consumer = consumer
        self._keep = keep
        self._workdir = workdir
        self._report = report
        self._created: list[str] = []
        self._pg_dump = _pg_tool(pg_bin, "pg_dump")
        self._pg_restore = _pg_tool(pg_bin, "pg_restore")
        self._check_versions()

    def environment(self) -> AbstractContextManager[None]:
        """ADR-0007: nothing in this drill may create schema except Alembic."""
        return scoped_environment(CLOUDSCALE_PG_SCHEMA="migrations")

    # -- infrastructure -------------------------------------------------------

    def _check_versions(self) -> None:
        try:
            with self._psycopg.connect(self._admin_dsn, connect_timeout=5) as conn:
                row = conn.execute(
                    "SELECT current_setting('server_version_num')"
                ).fetchone()
        except self._psycopg.OperationalError as error:
            raise ToolingError(f"cannot reach {self._admin_dsn}: {error}") from error
        if row is None:
            raise ToolingError("server_version_num query returned no row")
        server_major = int(row[0]) // 10_000
        for tool in (self._pg_dump, self._pg_restore):
            client_major = _pg_client_major(tool)
            if client_major != server_major:
                raise ToolingError(
                    f"{tool} is PostgreSQL {client_major} but the server is "
                    f"{server_major}; point --pg-bin / CLOUDSCALE_PG_BIN at a "
                    f"{server_major}.x client"
                )
        self._report.notes.append(
            f"postgresql server major {server_major}, client matched"
        )

    def create_database(self, label: str) -> str:
        name = f"{DRILL_PREFIX}{label}_{uuid.uuid4().hex[:10]}"
        with self._psycopg.connect(self._admin_dsn, autocommit=True) as admin:
            admin.execute(f'CREATE DATABASE "{name}"')
        self._created.append(name)
        return _with_database(self._admin_dsn, name)

    def cleanup(self) -> None:
        if self._keep:
            self._report.notes.append(f"kept databases: {self._created}")
            return
        with self._psycopg.connect(self._admin_dsn, autocommit=True) as admin:
            for name in self._created:
                admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        self._created.clear()

    def migrate_head(self, dsn: str) -> None:
        from alembic import command

        from cloudscale.entrypoints.migrate import alembic_config

        with scoped_environment(CLOUDSCALE_PG_DSN=dsn):
            command.upgrade(alembic_config(), "head")

    # -- seed -------------------------------------------------------------------

    def seed(self, dsn: str, *, accounts: int) -> dict[str, int]:
        from cloudscale.adapters.postgres.account_registry import (
            PostgresAccountRegistry,
        )
        from cloudscale.adapters.postgres.command_unit_of_work import (
            PostgresCommandUnitOfWork,
        )

        uow = PostgresCommandUnitOfWork(dsn, pool_max=1)
        future = PostgresCommandUnitOfWork(
            dsn, pool_max=1, clock=lambda: datetime.now(UTC) + timedelta(hours=2)
        )
        registry = PostgresAccountRegistry(dsn, pool_max=1)
        try:
            return seed_workload(
                uow, future, registry, lambda: self.rebuild(dsn), accounts=accounts
            )
        finally:
            registry.close()
            future.close()
            uow.close()

    # -- dump / restore ---------------------------------------------------------

    def dump(self, dsn: str) -> Path:
        target = self._workdir / f"{_database_of(dsn)}.dump"
        subprocess.run(  # noqa: S603 - fixed argv; dsn is the operator's own
            [
                self._pg_dump,
                "--format=custom",
                "--no-owner",
                "--file",
                str(target),
                dsn,
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=3_600,
        )
        return target

    def restore(self, dump_path: Path) -> str:
        dsn = self.create_database("restored")
        completed = subprocess.run(  # noqa: S603 - fixed argv
            [
                self._pg_restore,
                "--no-owner",
                "--no-privileges",
                "--exit-on-error",
                "--dbname",
                dsn,
                str(dump_path),
            ],
            capture_output=True,
            text=True,
            timeout=3_600,
        )
        if completed.returncode != 0:
            raise ToolingError(f"pg_restore failed: {completed.stderr.strip()[-2000:]}")
        return dsn

    def verify_revision(self, dsn: str) -> tuple[bool, str]:
        """Run the documented command, exactly as RUNBOOK R7 tells an operator to."""
        env = {**os.environ, "CLOUDSCALE_PG_DSN": dsn}
        completed = subprocess.run(  # noqa: S603 - fixed argv
            [sys.executable, "-m", "cloudscale.entrypoints.migrate", "current"],
            cwd=REPOSITORY_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
        )
        output = (completed.stdout + completed.stderr).strip()
        ok = completed.returncode == 0 and schema.CURRENT_REVISION in output
        revision_lines = [line for line in output.splitlines() if "Rev:" in line]
        return ok, (revision_lines[-1].strip() if revision_lines else output[-500:])

    def adapters_start(self, dsn: str) -> tuple[bool, str]:
        from cloudscale.adapters.postgres.command_unit_of_work import (
            PostgresCommandUnitOfWork,
        )
        from cloudscale.adapters.postgres.pool import SchemaNotMigratedError

        try:
            PostgresCommandUnitOfWork(dsn, pool_max=1).close()
        except SchemaNotMigratedError as error:
            return False, str(error)
        return True, "migrations mode accepted the restored revision"

    # -- read / truncate / rebuild --------------------------------------------

    def log_facts(self, dsn: str) -> LogFacts:
        with self._psycopg.connect(dsn) as conn:
            count_row = conn.execute(
                "SELECT COUNT(*), COALESCE(MAX(id), 0) FROM events"
            ).fetchone()
            streams = conn.execute(
                "SELECT stream, MAX(account_id) FROM events GROUP BY stream"
            ).fetchall()
            hold_rows = conn.execute(
                "SELECT type, transfer_id FROM events WHERE type = ANY(%s) "
                "AND transfer_id IS NOT NULL ORDER BY id",
                (list(HOLD_EVENT_TYPES),),
            ).fetchall()
        assert count_row is not None
        return LogFacts(
            events=int(count_row[0]),
            tail_id=int(count_row[1]),
            streams={str(s): str(a) for s, a in streams},
            open_hold_ids=_open_holds_from_rows((str(t), str(h)) for t, h in hold_rows),
        )

    def read_models(self, dsn: str) -> ReadModelSnapshot:
        with self._psycopg.connect(dsn) as conn:
            balances = conn.execute(
                "SELECT account_id, balance, held, version FROM balances"
            ).fetchall()
            holds = conn.execute(
                "SELECT hold_id, source, target, amount, expires_at, state FROM holds"
            ).fetchall()
            transfers = conn.execute(
                "SELECT transfer_id, kind, reverts, reverted_by FROM transfers"
            ).fetchall()
            legs = conn.execute(
                "SELECT transfer_id, account_id, amount, direction FROM transfer_legs"
            ).fetchall()
            dead = conn.execute("SELECT event_id FROM dead_letters").fetchall()
            processed = conn.execute("SELECT COUNT(*) FROM processed_events").fetchone()
        assert processed is not None
        return ReadModelSnapshot(
            balances={str(r[0]): (int(r[1]), int(r[2]), int(r[3])) for r in balances},
            holds=frozenset(tuple(r) for r in holds),
            transfers=frozenset(tuple(r) for r in transfers),
            transfer_legs=frozenset(tuple(r) for r in legs),
            dead_letter_ids=frozenset(str(r[0]) for r in dead),
            processed_events=int(processed[0]),
            open_hold_ids=frozenset(str(r[0]) for r in holds if r[5] == "open"),
        )

    def truncate_derived(self, dsn: str) -> list[str]:
        tables = sorted(schema.DERIVED)
        with self._psycopg.connect(dsn) as conn:
            conn.execute("TRUNCATE " + ", ".join(tables))
            conn.execute("UPDATE events SET published = false")
            conn.commit()
        return tables

    def fold_all(
        self, dsn: str, streams: dict[str, str]
    ) -> dict[str, tuple[int, int, int]]:
        from cloudscale.adapters.postgres.command_unit_of_work import (
            PostgresCommandUnitOfWork,
        )

        uow = PostgresCommandUnitOfWork(dsn, pool_max=1)
        try:
            return {
                account: _state_tuple(uow.fold_stream(account))
                for account in streams.values()
            }
        finally:
            uow.close()

    def rebuild(self, dsn: str) -> dict[str, int | bool | str | None]:
        from cloudscale.adapters.postgres.event_store import PostgresEventStore
        from cloudscale.adapters.postgres.projection_store import (
            PostgresProjectionStore,
        )

        feed = PostgresEventStore(dsn, pool_max=1)
        projection = PostgresProjectionStore(dsn, consumer=self._consumer, pool_max=1)
        try:
            return _drain(feed, projection)
        finally:
            projection.close()
            feed.close()


# -- SQLite tier ----------------------------------------------------------------------


class SqliteDrill:
    """Same steps over a log file and a projection file; the backup API is the dump."""

    def __init__(
        self, directory: Path, *, consumer: str, keep: bool, report: Report
    ) -> None:
        self._dir = directory
        self._consumer = consumer
        self._keep = keep
        self._report = report
        self._created: list[Path] = []

    def environment(self) -> AbstractContextManager[None]:
        return scoped_environment()

    def create_database(self, label: str) -> str:
        target = self._dir / f"{DRILL_PREFIX}{label}_{uuid.uuid4().hex[:10]}"
        target.mkdir(parents=True, exist_ok=False)
        self._created.append(target)
        return str(target)

    def cleanup(self) -> None:
        if self._keep:
            self._report.notes.append(
                f"kept directories: {[str(p) for p in self._created]}"
            )
            return
        for path in self._created:
            shutil.rmtree(path, ignore_errors=True)
        self._created.clear()

    def migrate_head(self, dsn: str) -> None:
        # SQLite has no Alembic: the adapters upgrade files in place on open.
        from cloudscale.adapters.sqlite_compat.command_unit_of_work import (
            SqliteCommandUnitOfWork,
        )
        from cloudscale.adapters.sqlite_compat.dead_letter_store import (
            DeadLetteringProjectionStore,
        )

        SqliteCommandUnitOfWork(self._log(dsn)).close()
        DeadLetteringProjectionStore(
            path=self._projection(dsn), consumer=self._consumer
        ).close()

    @staticmethod
    def _log(dsn: str) -> str:
        return str(Path(dsn) / "log.db")

    @staticmethod
    def _projection(dsn: str) -> str:
        return str(Path(dsn) / "projection.db")

    def seed(self, dsn: str, *, accounts: int) -> dict[str, int]:
        from cloudscale.adapters.sqlite_compat.account_registry import (
            SqliteAccountRegistry,
        )
        from cloudscale.adapters.sqlite_compat.command_unit_of_work import (
            SqliteCommandUnitOfWork,
        )

        uow = SqliteCommandUnitOfWork(self._log(dsn))
        future = SqliteCommandUnitOfWork(
            self._log(dsn), clock=lambda: datetime.now(UTC) + timedelta(hours=2)
        )
        registry = SqliteAccountRegistry(self._log(dsn))
        try:
            return seed_workload(
                uow, future, registry, lambda: self.rebuild(dsn), accounts=accounts
            )
        finally:
            registry.close()
            future.close()
            uow.close()

    def dump(self, dsn: str) -> Path:
        target = self._dir / f"dump_{uuid.uuid4().hex[:10]}"
        target.mkdir(parents=True, exist_ok=False)
        self._created.append(target)
        for name in ("log.db", "projection.db"):
            source = sqlite3.connect(str(Path(dsn) / name))
            destination = sqlite3.connect(str(target / name))
            try:
                source.backup(destination)  # consistent per file
            finally:
                destination.close()
                source.close()
        self._report.notes.append(
            "sqlite: the two files are backed up one after the other while quiescent; "
            "they are consistent with each other only because nothing wrote between"
        )
        return target

    def restore(self, dump_path: Path) -> str:
        dsn = self.create_database("restored")
        for name in ("log.db", "projection.db"):
            shutil.copyfile(dump_path / name, Path(dsn) / name)
        return dsn

    def verify_revision(self, dsn: str) -> tuple[bool, str]:
        return (
            True,
            "not applicable on SQLite: the adapters upgrade the schema in place",
        )

    def adapters_start(self, dsn: str) -> tuple[bool, str]:
        self.migrate_head(dsn)
        return True, "adapters opened both files"

    def log_facts(self, dsn: str) -> LogFacts:
        conn = sqlite3.connect(self._log(dsn))
        try:
            count_row = conn.execute(
                "SELECT COUNT(*), COALESCE(MAX(id), 0) FROM events"
            ).fetchone()
            streams = conn.execute(
                "SELECT stream, MAX(account_id) FROM events GROUP BY stream"
            ).fetchall()
            placeholders = ", ".join("?" for _ in HOLD_EVENT_TYPES)
            hold_rows = conn.execute(
                f"SELECT type, transfer_id FROM events WHERE type IN ({placeholders}) "  # noqa: S608 - placeholders only
                "AND transfer_id IS NOT NULL ORDER BY id",
                HOLD_EVENT_TYPES,
            ).fetchall()
        finally:
            conn.close()
        return LogFacts(
            events=int(count_row[0]),
            tail_id=int(count_row[1]),
            streams={str(s): str(a) for s, a in streams},
            open_hold_ids=_open_holds_from_rows((str(t), str(h)) for t, h in hold_rows),
        )

    def read_models(self, dsn: str) -> ReadModelSnapshot:
        conn = sqlite3.connect(self._projection(dsn))
        try:
            balances = conn.execute(
                "SELECT account_id, balance, held, version FROM balances"
            ).fetchall()
            holds = conn.execute(
                "SELECT hold_id, source, target, amount, expires_at, state FROM holds"
            ).fetchall()
            transfers = conn.execute(
                "SELECT transfer_id, kind, reverts, reverted_by FROM transfers"
            ).fetchall()
            legs = conn.execute(
                "SELECT transfer_id, account_id, amount, direction FROM transfer_legs"
            ).fetchall()
            dead = conn.execute("SELECT event_id FROM dead_letters").fetchall()
            processed = conn.execute("SELECT COUNT(*) FROM processed_events").fetchone()
        finally:
            conn.close()
        return ReadModelSnapshot(
            balances={str(r[0]): (int(r[1]), int(r[2]), int(r[3])) for r in balances},
            holds=frozenset(tuple(r) for r in holds),
            transfers=frozenset(tuple(r) for r in transfers),
            transfer_legs=frozenset(tuple(r) for r in legs),
            dead_letter_ids=frozenset(str(r[0]) for r in dead),
            processed_events=int(processed[0]),
            open_hold_ids=frozenset(str(r[0]) for r in holds if r[5] == "open"),
        )

    def truncate_derived(self, dsn: str) -> list[str]:
        truncated: list[str] = []
        for path, tables in (
            (self._log(dsn), ("stream_snapshots",)),
            (self._projection(dsn), schema.CONSUMER_REBUILT),
        ):
            conn = sqlite3.connect(path)
            try:
                for table in tables:
                    if table not in schema.DERIVED:
                        raise ToolingError(
                            f"refusing to truncate non-derived table {table}"
                        )
                    conn.execute(f"DELETE FROM {table}")  # noqa: S608 - name from the DERIVED set
                    truncated.append(table)
                conn.commit()
            finally:
                conn.close()
        return truncated

    def fold_all(
        self, dsn: str, streams: dict[str, str]
    ) -> dict[str, tuple[int, int, int]]:
        from cloudscale.adapters.sqlite_compat.command_unit_of_work import (
            SqliteCommandUnitOfWork,
        )

        uow = SqliteCommandUnitOfWork(self._log(dsn))
        try:
            return {
                account: _state_tuple(uow.fold_stream(account))
                for account in streams.values()
            }
        finally:
            uow.close()

    def rebuild(self, dsn: str) -> dict[str, int | bool | str | None]:
        from cloudscale.adapters.sqlite_compat.dead_letter_store import (
            DeadLetteringProjectionStore,
        )
        from cqrs import SqliteEventStore

        feed = SqliteEventStore(self._log(dsn))
        projection = DeadLetteringProjectionStore(
            path=self._projection(dsn), consumer=self._consumer
        )
        try:
            return _drain(feed, projection)
        finally:
            projection.close()
            feed.close()


# -- shared mechanics -----------------------------------------------------------------


def _state_tuple(state: Any) -> tuple[int, int, int]:
    return int(state.balance), int(state.held), int(state.version)


def _drain(feed: Any, projection: Any) -> dict[str, int | bool | str | None]:
    """Run the resilient consumer until the log is drained or it halts."""
    applied = dead_lettered = duplicates = 0
    consumer = ResilientConsumer(feed, projection, batch=CONSUMER_BATCH)
    while True:
        report = consumer.run()
        applied += report.applied
        dead_lettered += report.dead_lettered
        duplicates += report.duplicates
        if report.halted or (report.applied == 0 and report.dead_lettered == 0):
            return {
                "applied": applied,
                "dead_lettered": dead_lettered,
                "duplicates": duplicates,
                "halted": report.halted,
                "halt_reason": report.halt_reason,
            }


def _diff(label: str, before: Any, after: Any) -> str:
    if before == after:
        return f"{label}: {len(after) if hasattr(after, '__len__') else after} equal"
    if isinstance(before, dict) and isinstance(after, dict):
        keys = sorted(
            k for k in set(before) | set(after) if before.get(k) != after.get(k)
        )
        return f"{label}: differ on {keys[:10]}"
    if isinstance(before, frozenset | set) and isinstance(after, frozenset | set):
        return f"{label}: only-before={len(before - after)} only-after={len(after - before)}"
    return f"{label}: before={before!r} after={after!r}"


def run_drill(
    tier: PostgresDrill | SqliteDrill,
    report: Report,
    *,
    seeded: bool,
    dump_path: Path | None,
    accounts: int,
    source_log_tail: Callable[[], int | None],
) -> Report:
    source_dsn: str | None = None
    with tier.environment():
        return _run_drill(
            tier,
            report,
            seeded=seeded,
            dump_path=dump_path,
            accounts=accounts,
            source_log_tail=source_log_tail,
            source_dsn=source_dsn,
        )


def _run_drill(
    tier: PostgresDrill | SqliteDrill,
    report: Report,
    *,
    seeded: bool,
    dump_path: Path | None,
    accounts: int,
    source_log_tail: Callable[[], int | None],
    source_dsn: str | None,
) -> Report:
    try:
        if seeded:
            source_dsn = tier.create_database("source")
            _timed(report, "seed_migrate", lambda: tier.migrate_head(source_dsn))
            counts = _timed(
                report, "seed", lambda: tier.seed(source_dsn, accounts=accounts)
            )
            report.notes.append(f"seed: {counts}")
            dump_path = _timed(report, "dump", lambda: tier.dump(source_dsn))
        assert dump_path is not None
        report.dump_bytes = sum(
            p.stat().st_size
            for p in ([dump_path] if dump_path.is_file() else dump_path.iterdir())
        )

        restored = _timed(report, "restore", lambda: tier.restore(dump_path))

        ok, detail = _timed(
            report, "verify_revision", lambda: tier.verify_revision(restored)
        )
        report.criterion("schema_revision_matches_build", ok, detail)
        ok, detail = tier.adapters_start(restored)
        report.criterion("adapters_start_in_migrations_mode", ok, detail)
        if not report.criteria["schema_revision_matches_build"]["pass"] or not ok:
            return report  # nothing below is meaningful on the wrong schema

        facts = tier.log_facts(restored)
        report.events, report.streams = facts.events, len(facts.streams)
        before = tier.read_models(restored)

        truncated = tier.truncate_derived(restored)
        report.notes.append(f"truncated: {truncated}")
        folds = _timed(
            report, "full_fold", lambda: tier.fold_all(restored, facts.streams)
        )
        drained = _timed(report, "rebuild", lambda: tier.rebuild(restored))
        report.consumer = drained
        rebuild_seconds = report.durations_seconds["rebuild"]
        if rebuild_seconds > 0:
            report.rebuild_events_per_second = round(facts.events / rebuild_seconds, 1)
        after = tier.read_models(restored)

        report.criterion(
            "consumer_did_not_halt",
            not drained["halted"],
            f"halted={drained['halted']} reason={drained['halt_reason']}",
        )
        report.criterion(
            "rebuilt_balances_equal_full_fold",
            after.balances == folds,
            _diff("balances vs fold", folds, after.balances),
        )
        report.criterion(
            "rebuilt_balances_equal_dump",
            after.balances == before.balances,
            _diff("balances", before.balances, after.balances),
        )
        report.criterion(
            "rebuilt_holds_equal_dump",
            after.holds == before.holds,
            _diff("holds", before.holds, after.holds),
        )
        report.criterion(
            "rebuilt_transfers_equal_dump",
            after.transfers == before.transfers
            and after.transfer_legs == before.transfer_legs,
            _diff("transfers", before.transfers, after.transfers)
            + "; "
            + _diff("transfer_legs", before.transfer_legs, after.transfer_legs),
        )
        report.criterion(
            "conservation_total_balance_unchanged",
            before.total_balance
            == after.total_balance
            == sum(b for b, _h, _v in folds.values()),
            f"before={before.total_balance} after={after.total_balance}",
        )
        report.criterion(
            "open_holds_equal_log",
            after.open_hold_ids == facts.open_hold_ids,
            _diff("open holds", facts.open_hold_ids, after.open_hold_ids),
        )
        report.criterion(
            "processed_events_equal_events",
            after.processed_events == facts.events,
            f"processed={after.processed_events} events={facts.events}",
        )
        report.criterion(
            "dead_letters_subset_of_dump",
            after.dead_letter_ids <= before.dead_letter_ids,
            _diff("dead letters", before.dead_letter_ids, after.dead_letter_ids),
        )

        tail_now = source_log_tail()
        report.rpo_events = (
            None if tail_now is None else max(0, tail_now - facts.tail_id)
        )
        if seeded:
            report.rpo_events = 0
        return report
    finally:
        tier.cleanup()


# -- CLI ---------------------------------------------------------------------------------


def _parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    where = parser.add_mutually_exclusive_group(required=True)
    where.add_argument(
        "--dsn", help="administrative postgresql:// DSN (may CREATE DATABASE)"
    )
    where.add_argument("--sqlite-dir", help="directory for the SQLite drill's files")
    what = parser.add_mutually_exclusive_group(required=True)
    what.add_argument(
        "--seeded", action="store_true", help="seed a throwaway database, then drill it"
    )
    what.add_argument("--dump", help="drill an existing pg_dump custom-format file")
    parser.add_argument(
        "--source-dsn",
        help="with --dump: the live database the dump came from, to measure rpo_events",
    )
    parser.add_argument(
        "--pg-bin", help="directory holding pg_dump/pg_restore of the server's major"
    )
    parser.add_argument(
        "--consumer", default="balances", help="consumer name (default: balances)"
    )
    parser.add_argument(
        "--accounts", type=int, default=8, help="seed accounts (>= 8, default 8)"
    )
    parser.add_argument(
        "--report",
        help="report path (default: evidence/<sha>/restore-drill/report.json)",
    )
    parser.add_argument(
        "--keep", action="store_true", help="keep the databases the drill created"
    )
    parser.add_argument(
        "--quiet", action="store_true", help="print only the verdict line"
    )
    args = parser.parse_args(arguments)
    if args.sqlite_dir and args.dump:
        parser.error("--dump applies to PostgreSQL; use --seeded with --sqlite-dir")
    if args.source_dsn and not args.dump:
        parser.error("--source-dsn only makes sense with --dump")
    return args


def _report_path(explicit: str | None, sha: str | None) -> Path:
    if explicit:
        return Path(explicit)
    return (
        REPOSITORY_ROOT
        / "evidence"
        / (sha or "unknown")
        / "restore-drill"
        / "report.json"
    )


def _source_tail_reader(source_dsn: str | None) -> Callable[[], int | None]:
    if not source_dsn:
        return lambda: None

    def read() -> int | None:
        import psycopg

        with psycopg.connect(source_dsn, connect_timeout=5) as conn:
            row = conn.execute("SELECT COALESCE(MAX(id), 0) FROM events").fetchone()
        return None if row is None else int(row[0])

    return read


def main(arguments: Sequence[str] | None = None) -> int:
    args = _parse_args(arguments)
    sha = _git_sha()
    if args.dsn:
        report = Report(
            tier="postgresql", mode="seeded" if args.seeded else "dump", git_sha=sha
        )
        workdir = Path(tempfile.mkdtemp(prefix=DRILL_PREFIX))
    else:
        report = Report(tier="sqlite", mode="seeded", git_sha=sha)
        workdir = Path(args.sqlite_dir)
        workdir.mkdir(parents=True, exist_ok=True)

    try:
        tier: PostgresDrill | SqliteDrill
        if args.dsn:
            tier = PostgresDrill(
                args.dsn,
                pg_bin=_resolve_pg_bin(args.pg_bin),
                consumer=args.consumer,
                keep=args.keep,
                workdir=workdir,
                report=report,
            )
        else:
            tier = SqliteDrill(
                workdir, consumer=args.consumer, keep=args.keep, report=report
            )
        run_drill(
            tier,
            report,
            seeded=args.seeded,
            dump_path=Path(args.dump) if args.dump else None,
            accounts=args.accounts,
            source_log_tail=_source_tail_reader(args.source_dsn),
        )
    except (ToolingError, SeedError) as error:
        print(f"restore drill could not run: {error}", file=sys.stderr)
        return EXIT_TOOLING
    finally:
        if args.dsn and not args.keep:
            shutil.rmtree(workdir, ignore_errors=True)

    path = _report_path(args.report, sha)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report.to_json() + "\n")
    if not args.quiet:
        print(report.to_json())
    failed = sorted(name for name, c in report.criteria.items() if not c["pass"])
    verdict = "PASS" if report.passed else f"FAIL {failed}"
    print(
        f"restore drill {verdict}: {report.events} events, {report.streams} streams, "
        f"rebuild {report.durations_seconds.get('rebuild', 0):.2f}s, report {path}"
    )
    return EXIT_OK if report.passed else EXIT_FAILED


if __name__ == "__main__":
    raise SystemExit(main())
