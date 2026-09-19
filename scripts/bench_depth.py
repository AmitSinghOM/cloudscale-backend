"""Depth benchmark for ADR-0012: command latency against stream depth.

For each depth *d*, build a stream of *d* deposits, then time 200 further
``Withdraw(1)`` commands on that stream (each folds the stream, decides, and
appends). Run once with snapshots disabled (``--every 0``) and once with the
default interval, on either tier::

    python scripts/bench_depth.py sqlite --every 0
    python scripts/bench_depth.py sqlite --every 100
    python scripts/bench_depth.py postgresql://localhost/cloudscale_bench --every 0

Prints a Markdown table (p50 / p99 in ms per depth) suitable for pasting
into the ADR. The seeding phase is *not* timed. On PostgreSQL the target
database must exist and be empty; the script creates schema in ``auto`` mode
because a benchmark database is disposable by definition.

Not part of the gate: it is a measurement tool, and its numbers depend on
the host.
"""

from __future__ import annotations

import argparse
import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Sequence
from uuid import uuid4

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from cloudscale.application.command_service import normalize_command  # noqa: E402
from cloudscale.domain.commands import Deposit, Withdraw  # noqa: E402

DEPTHS = (1, 1_001, 2_000, 20_000)
SAMPLES = 200


def _request(command):
    return normalize_command(
        command,
        command_id=uuid4(),
        correlation_id=uuid4(),
        issuer="cloudscale",
        subject="bench",
    )


def _open(target: str, every: int) -> Any:
    if target.startswith(("postgresql://", "postgres://")):
        from cloudscale.adapters.postgres.command_unit_of_work import (
            PostgresCommandUnitOfWork,
        )

        return PostgresCommandUnitOfWork(target, snapshot_every=every, pool_max=1)
    from cloudscale.adapters.sqlite_compat.command_unit_of_work import (
        SqliteCommandUnitOfWork,
    )

    path = target
    if target == "sqlite":
        path = str(Path(tempfile.mkdtemp(prefix="cloudscale-bench-")) / "log.db")
    return SqliteCommandUnitOfWork(path, snapshot_every=every)


def _percentile(values: list[float], fraction: float) -> float:
    values = sorted(values)
    index = min(len(values) - 1, max(0, round(fraction * (len(values) - 1))))
    return values[index]


def bench(target: str, every: int, depths: Sequence[int], samples: int) -> list[dict]:
    rows = []
    for depth in depths:
        account = f"bench-{depth}-{uuid4().hex[:6]}"
        uow = _open(target, every)
        try:
            version = 0
            # Seed: each deposit funds every timed withdraw, so depth 1 never runs dry.
            for _ in range(depth):
                version = (
                    uow.execute(
                        _request(Deposit(account, samples, version))
                    ).committed_version
                    or 0
                )
            durations: list[float] = []
            for _ in range(samples):
                started = time.perf_counter()
                result = uow.execute(_request(Withdraw(account, 1, version)))
                durations.append((time.perf_counter() - started) * 1000)
                if result.committed_version is None:
                    raise RuntimeError(
                        f"benchmark command rejected: {result.error_code}"
                    )
                version = result.committed_version
        finally:
            uow.close()
        rows.append(
            {
                "depth": depth,
                "p50_ms": round(statistics.median(durations), 3),
                "p99_ms": round(_percentile(durations, 0.99), 3),
            }
        )
        print(
            f"depth={depth:>6} p50={rows[-1]['p50_ms']:>8} p99={rows[-1]['p99_ms']:>8} ms",
            file=sys.stderr,
        )
    return rows


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "target", help="'sqlite' (temp file), a SQLite path, or a postgresql:// DSN"
    )
    parser.add_argument(
        "--every", type=int, default=100, help="snapshot interval; 0 disables"
    )
    parser.add_argument("--depths", type=int, nargs="+", default=list(DEPTHS))
    parser.add_argument("--samples", type=int, default=SAMPLES)
    args = parser.parse_args(arguments)
    rows = bench(args.target, args.every, args.depths, args.samples)
    tier = "PostgreSQL" if args.target.startswith("postgres") else "SQLite"
    print("| Tier | snapshot_every | depth | p50 ms | p99 ms |")
    print("|---|---:|---:|---:|---:|")
    for row in rows:
        print(
            f"| {tier} | {args.every} | {row['depth']:,} | {row['p50_ms']} | {row['p99_ms']} |"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
