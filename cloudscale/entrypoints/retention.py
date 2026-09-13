"""Retention: bound the tables that otherwise grow forever.

What is pruned and why it is safe:

* ``command_results`` older than the idempotency window. A retried
  ``command_id`` older than the window is treated as new — so the window
  must exceed any client's realistic retry horizon (default 7 days).
* ``rate_limit_buckets`` not touched for longer than one full refill. A
  pruned bucket is recreated full on next use, which is exactly the state a
  refilled bucket would be in — no behaviour change.
* ``dead_letters`` are **evidence**, not garbage: pruned only when a TTL is
  explicitly given, and only letters older than it.

What is NOT pruned: the event log (source of record), ``event_envelopes``,
``processed_events`` (needed for exactly-once on replay), ``accounts``.
Event-stream snapshots are a separate design item (ROADMAP).

Deletes run in bounded batches so a long-neglected table cannot hold a
lock for the whole pass. Run from cron / a scheduled job::

    CLOUDSCALE_STORAGE=postgres CLOUDSCALE_PG_DSN=... \\
        python -m cloudscale.entrypoints.retention --command-results-days 7

Exit 0 on success; prints one JSON line with what was deleted.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

__all__ = ["RetentionPolicy", "RetentionReport", "prune_postgres", "prune_sqlite"]


@dataclass(frozen=True)
class RetentionPolicy:
    command_results_days: float = 7.0
    rate_limit_idle_seconds: float = 3600.0
    dead_letters_days: float | None = None  # None = never prune
    batch_size: int = 5_000

    def __post_init__(self) -> None:
        if self.command_results_days <= 0:
            raise ValueError("command_results_days must be positive")
        if self.rate_limit_idle_seconds <= 0:
            raise ValueError("rate_limit_idle_seconds must be positive")
        if self.dead_letters_days is not None and self.dead_letters_days <= 0:
            raise ValueError("dead_letters_days must be positive when set")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")


@dataclass
class RetentionReport:
    command_results: int = 0
    rate_limit_buckets: int = 0
    dead_letters: int = 0
    batches: int = 0


def _batched(
    execute_batch: Callable[[], int], report: RetentionReport, field: str
) -> None:
    """Run ``execute_batch()`` until it deletes fewer rows than a full batch."""
    while True:
        deleted = execute_batch()
        report.batches += 1
        setattr(report, field, getattr(report, field) + deleted)
        if deleted == 0:
            return


# -- PostgreSQL ----------------------------------------------------------------------


def prune_postgres(
    conninfo: str, policy: RetentionPolicy, *, now: datetime | None = None
) -> RetentionReport:
    import psycopg

    moment = now or datetime.now(UTC)
    report = RetentionReport()
    with psycopg.connect(conninfo, autocommit=True) as conn:
        cutoff = moment - timedelta(days=policy.command_results_days)

        def results_batch() -> int:
            return conn.execute(
                "DELETE FROM command_results WHERE command_id IN ("
                "  SELECT command_id FROM command_results WHERE created_at < %s"
                "  ORDER BY created_at LIMIT %s)",
                (cutoff, policy.batch_size),
            ).rowcount

        _batched(results_batch, report, "command_results")

        def table_exists(name: str) -> bool:
            # Adapters create their own tables lazily: rate_limit_buckets exists
            # only with the postgres limiter backend, dead_letters only once a
            # projection store has run. A missing table is "nothing to prune",
            # not a failure - found by the soak harness, which runs without
            # the postgres limiter.
            row = conn.execute(
                "SELECT to_regclass(%s) IS NOT NULL AS present", (name,)
            ).fetchone()
            return bool(row and row[0])

        idle_before = moment.timestamp() - policy.rate_limit_idle_seconds

        def buckets_batch() -> int:
            return conn.execute(
                "DELETE FROM rate_limit_buckets WHERE bucket_key IN ("
                "  SELECT bucket_key FROM rate_limit_buckets WHERE updated_at < %s"
                "  LIMIT %s)",
                (idle_before, policy.batch_size),
            ).rowcount

        if table_exists("rate_limit_buckets"):
            _batched(buckets_batch, report, "rate_limit_buckets")

        if policy.dead_letters_days is not None and table_exists("dead_letters"):
            dlq_cutoff = (moment - timedelta(days=policy.dead_letters_days)).isoformat()

            def letters_batch() -> int:
                # dead_lettered_at is ISO-8601 text on both tiers; lexical
                # order == chronological order for same-offset UTC stamps.
                return conn.execute(
                    "DELETE FROM dead_letters WHERE event_id IN ("
                    "  SELECT event_id FROM dead_letters WHERE dead_lettered_at < %s"
                    "  LIMIT %s)",
                    (dlq_cutoff, policy.batch_size),
                ).rowcount

            _batched(letters_batch, report, "dead_letters")
    return report


# -- SQLite --------------------------------------------------------------------------


def prune_sqlite(
    log_db: str,
    projection_db: str,
    policy: RetentionPolicy,
    *,
    now: datetime | None = None,
) -> RetentionReport:
    moment = now or datetime.now(UTC)
    report = RetentionReport()

    log = sqlite3.connect(log_db, timeout=5.0)
    try:
        cutoff = (moment - timedelta(days=policy.command_results_days)).timestamp()

        def results_batch() -> int:
            with log:
                return log.execute(
                    "DELETE FROM command_results WHERE rowid IN ("
                    "  SELECT rowid FROM command_results WHERE created_at < ?"
                    "  ORDER BY created_at LIMIT ?)",
                    (cutoff, policy.batch_size),
                ).rowcount

        _batched(results_batch, report, "command_results")
    finally:
        log.close()

    # The SQLite tier's rate limiter is in-memory; nothing to prune.

    if policy.dead_letters_days is not None:
        projection = sqlite3.connect(projection_db, timeout=5.0)
        try:
            dlq_cutoff = (moment - timedelta(days=policy.dead_letters_days)).isoformat()

            has_table = projection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='dead_letters'"
            ).fetchone()

            def letters_batch() -> int:
                if not has_table:  # projection never dead-lettered: nothing to prune
                    return 0
                with projection:
                    return projection.execute(
                        "DELETE FROM dead_letters WHERE rowid IN ("
                        "  SELECT rowid FROM dead_letters WHERE dead_lettered_at < ?"
                        "  LIMIT ?)",
                        (dlq_cutoff, policy.batch_size),
                    ).rowcount

            _batched(letters_batch, report, "dead_letters")
        finally:
            projection.close()
    return report


# -- CLI -----------------------------------------------------------------------------


def _parse(arguments: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--command-results-days", type=float, default=7.0)
    parser.add_argument("--rate-limit-idle-seconds", type=float, default=3600.0)
    parser.add_argument(
        "--dead-letters-days",
        type=float,
        default=None,
        help="prune dead letters older than this; omitted = never",
    )
    parser.add_argument("--batch-size", type=int, default=5_000)
    return parser.parse_args(arguments)


def main(arguments: Sequence[str] | None = None) -> int:
    args = _parse(arguments)
    policy = RetentionPolicy(
        command_results_days=args.command_results_days,
        rate_limit_idle_seconds=args.rate_limit_idle_seconds,
        dead_letters_days=args.dead_letters_days,
        batch_size=args.batch_size,
    )
    storage = os.environ.get("CLOUDSCALE_STORAGE", "sqlite")
    if storage == "postgres":
        report = prune_postgres(os.environ["CLOUDSCALE_PG_DSN"], policy)
    elif storage == "sqlite":
        report = prune_sqlite(
            os.environ["CLOUDSCALE_LOG_DB"],
            os.environ["CLOUDSCALE_PROJECTION_DB"],
            policy,
        )
    else:
        raise ValueError(f"unsupported CLOUDSCALE_STORAGE: {storage!r}")
    print(json.dumps({"storage": storage, "policy": asdict(policy), **asdict(report)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
