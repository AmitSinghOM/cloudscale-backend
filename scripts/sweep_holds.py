"""Expire holds past ``expires_at`` (ADR-0014): the only clock in the hold lifecycle.

Usage::

    python scripts/sweep_holds.py --log <sqlite-log-path> --projection <sqlite-projection-path>
    python scripts/sweep_holds.py --dsn postgresql://...      # both on one database

For every open hold whose ``expires_at`` <= now (read from the ``holds`` read
model), executes ``ExpireHold`` through the command unit of work with a
**deterministic** command id, ``uuid5(HOLD_EXPIRY_NAMESPACE, hold_id)``. Two
sweepers racing, or a sweeper racing a post/void, resolve through the same
``command_results`` claim every command uses: exactly one appends
``HoldReleased(expired)``; the other sees the stored result, a
``command_id_conflict`` (its fold saw a different version), or
``hold_not_open``. Nothing about expiry lives in the fold, so replay stays
deterministic.

Exit codes: 0 (prints ``expired=N skipped=M``); 2 database not found /
not migrated. Idempotent: run it as often as you like. ``expired`` counts
ACCEPTED results, which include a replay of this very command when a
concurrent sweeper already applied it at the same stream version (the stored
result is returned byte-for-byte and cannot be told apart); the log carries
exactly one ``HoldReleased(expired)`` per hold regardless of the counters.

On PostgreSQL the tool never creates schema (ADR-0007): unless the operator
set ``CLOUDSCALE_PG_SCHEMA`` explicitly, ``migrations`` mode is forced.
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from cloudscale.adapters.postgres.pool import SchemaNotMigratedError  # noqa: E402
from cloudscale.application.command_service import normalize_command  # noqa: E402
from cloudscale.domain.commands import ExpireHold  # noqa: E402
from cloudscale.domain.results import CommandOutcome  # noqa: E402

#: Fixed namespace so every sweeper derives the same command id for a hold.
HOLD_EXPIRY_NAMESPACE = uuid.UUID("5b1c6f2e-8d3a-4e7b-9c1d-2f3a4b5c6d7e")
SWEEPER_SUBJECT = "hold-sweeper"
EXIT_NOT_FOUND = 2


def expiry_command_id(hold_id: str) -> uuid.UUID:
    return uuid.uuid5(HOLD_EXPIRY_NAMESPACE, hold_id)


def sweep(
    unit_of_work: Any,
    projection: Any,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> tuple[int, int]:
    """Expire every open hold past its expiry. Returns ``(expired, skipped)``."""
    expired = skipped = 0
    for hold in projection.open_holds_expired_at(now().isoformat()):
        hold_id = str(hold["hold_id"])
        source = str(hold["source"])
        version = unit_of_work.fold_stream(source).version
        request = normalize_command(
            ExpireHold(source, uuid.UUID(hold_id), version),
            command_id=expiry_command_id(hold_id),
            correlation_id=uuid.uuid4(),
            issuer="cloudscale",
            subject=SWEEPER_SUBJECT,
        )
        result = unit_of_work.execute(request)
        if result.outcome is CommandOutcome.ACCEPTED:
            expired += 1
        else:
            # Lost the race to a post/void/another sweeper, or the version
            # moved under us; the next sweep re-reads the read model.
            skipped += 1
    return expired, skipped


def _open(args: argparse.Namespace) -> tuple[Any, Any]:
    if args.dsn:
        os.environ.setdefault("CLOUDSCALE_PG_SCHEMA", "migrations")
        from cloudscale.adapters.postgres.command_unit_of_work import (
            PostgresCommandUnitOfWork,
        )
        from cloudscale.adapters.postgres.projection_store import (
            PostgresProjectionStore,
        )

        return (
            PostgresCommandUnitOfWork(args.dsn, pool_max=1),
            PostgresProjectionStore(args.dsn, pool_max=1),
        )
    for path in (args.log, args.projection):
        if not Path(path).exists():
            raise FileNotFoundError(path)
    from cloudscale.adapters.sqlite_compat.command_unit_of_work import (
        SqliteCommandUnitOfWork,
    )
    from cloudscale.adapters.sqlite_compat.dead_letter_store import (
        DeadLetteringProjectionStore,
    )

    return SqliteCommandUnitOfWork(args.log), DeadLetteringProjectionStore(
        path=args.projection
    )


def _parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn", help="postgresql:// DSN holding both log and read model"
    )
    parser.add_argument("--log", help="SQLite event-log path")
    parser.add_argument("--projection", help="SQLite projection path")
    args = parser.parse_args(arguments)
    if bool(args.dsn) == bool(args.log or args.projection):
        parser.error("give --dsn, or both --log and --projection")
    if not args.dsn and not (args.log and args.projection):
        parser.error("--log and --projection are both required for SQLite")
    return args


def main(arguments: Sequence[str] | None = None) -> int:
    args = _parse_args(arguments)
    try:
        unit_of_work, projection = _open(args)
    except FileNotFoundError as missing:
        print(f"database not found: {missing}", file=sys.stderr)
        return EXIT_NOT_FOUND
    except SchemaNotMigratedError as error:
        print(f"not a migrated cloudscale database: {error}", file=sys.stderr)
        return EXIT_NOT_FOUND
    try:
        expired, skipped = sweep(unit_of_work, projection)
    finally:
        unit_of_work.close()
        projection.close()
    print(f"expired={expired} skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
