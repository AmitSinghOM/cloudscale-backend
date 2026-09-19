"""Operator tool for stream snapshots (ADR-0012): inspect or drop the cache.

Usage::

    python scripts/snapshots.py <sqlite-log-path | postgresql://dsn> stats
    python scripts/snapshots.py <target> drop <account_id>
    python scripts/snapshots.py <target> drop --all

Snapshots are derived data: dropping them is always safe; the next command
on a stream folds it in full once and writes a fresh row. RUNBOOK R10 uses
``drop`` as the first step when a balance disagrees with a full replay -- if
the disagreement persists after the drop, the snapshot was not the cause.

On PostgreSQL the tool never creates schema (ADR-0007): unless the operator
set ``CLOUDSCALE_PG_SCHEMA`` explicitly, ``migrations`` mode is forced and an
unmigrated database is reported as exit 2.

Exit codes: 0 success; 2 database not found / not migrated.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path
from typing import Protocol, Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from cloudscale.adapters.postgres.pool import SchemaNotMigratedError  # noqa: E402

EXIT_NOT_FOUND = 2


class SnapshotStore(Protocol):
    def stats(self) -> dict: ...
    def drop(self, stream: str) -> int: ...
    def drop_all(self) -> int: ...
    def close(self) -> None: ...


class _SqliteSnapshots:
    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path)

    def stats(self) -> dict:
        row = self._conn.execute(
            "SELECT COUNT(*), COALESCE(MIN(seq), 0), COALESCE(MAX(seq), 0) "
            "FROM stream_snapshots"
        ).fetchone()
        (streams,) = self._conn.execute(
            "SELECT COUNT(DISTINCT stream) FROM events"
        ).fetchone()
        return {
            "snapshots": row[0],
            "streams_with_events": streams,
            "min_seq": row[1],
            "max_seq": row[2],
        }

    def drop(self, stream: str) -> int:
        with self._conn:
            return self._conn.execute(
                "DELETE FROM stream_snapshots WHERE stream = ?", (stream,)
            ).rowcount

    def drop_all(self) -> int:
        with self._conn:
            return self._conn.execute("DELETE FROM stream_snapshots").rowcount

    def close(self) -> None:
        self._conn.close()


class _PostgresSnapshots:
    def __init__(self, dsn: str) -> None:
        from cloudscale.adapters.postgres import schema
        from cloudscale.adapters.postgres.pool import ensure_schema, open_pool

        self._pool = open_pool(dsn, max_size=1)
        ensure_schema(self._pool, schema.SNAPSHOTS)

    def stats(self) -> dict:
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n, COALESCE(MIN(seq), 0) AS lo, "
                "COALESCE(MAX(seq), 0) AS hi FROM stream_snapshots"
            ).fetchone()
            streams = conn.execute(
                "SELECT COUNT(DISTINCT stream) AS n FROM events"
            ).fetchone()
        assert row is not None and streams is not None  # aggregates always return a row
        return {
            "snapshots": row["n"],
            "streams_with_events": streams["n"],
            "min_seq": row["lo"],
            "max_seq": row["hi"],
        }

    def drop(self, stream: str) -> int:
        with self._pool.connection() as conn:
            return conn.execute(
                "DELETE FROM stream_snapshots WHERE stream = %s", (stream,)
            ).rowcount

    def drop_all(self) -> int:
        with self._pool.connection() as conn:
            return conn.execute("DELETE FROM stream_snapshots").rowcount

    def close(self) -> None:
        self._pool.close()


def _is_postgres(target: str) -> bool:
    return target.startswith(("postgresql://", "postgres://"))


def _open(target: str) -> SnapshotStore:
    if _is_postgres(target):
        os.environ.setdefault("CLOUDSCALE_PG_SCHEMA", "migrations")
        return _PostgresSnapshots(target)
    if not Path(target).exists():
        raise FileNotFoundError(target)
    return _SqliteSnapshots(target)


def _parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", help="SQLite event-log path, or a postgresql:// DSN")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("stats", help="count snapshots and streams")
    drop = sub.add_parser("drop", help="delete snapshot rows (always safe)")
    drop.add_argument("account_id", nargs="?", help="account whose snapshot to drop")
    drop.add_argument("--all", action="store_true", help="drop every snapshot")
    args = parser.parse_args(arguments)
    if args.command == "drop" and bool(args.account_id) == args.all:
        parser.error("drop takes exactly one of <account_id> or --all")
    return args


def main(arguments: Sequence[str] | None = None) -> int:
    args = _parse_args(arguments)
    try:
        store = _open(args.target)
    except FileNotFoundError:
        print(f"database not found: {args.target}", file=sys.stderr)
        return EXIT_NOT_FOUND
    except SchemaNotMigratedError as error:
        print(f"not a migrated cloudscale database: {error}", file=sys.stderr)
        return EXIT_NOT_FOUND
    try:
        if args.command == "stats":
            for key, value in store.stats().items():
                print(f"{key}: {value}")
            return 0
        dropped = (
            store.drop_all() if args.all else store.drop(f"account-{args.account_id}")
        )
        print(f"dropped {dropped} snapshot(s)")
        return 0
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
