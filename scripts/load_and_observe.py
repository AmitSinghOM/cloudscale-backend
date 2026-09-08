#!/usr/bin/env python3
"""Phase 3 load harness: measure the real local pipeline, stdlib-only.

Drives the actual durable write path (CommandHandler over a file-backed
SqliteEventStore), the resilient consumer catch-up, and the query side, then
writes a revision-bound JSON report with p50/p95/p99 latencies and throughput
under ``evidence/<revision>/phase-3-load/``.

Two write segments are measured separately on purpose:

- ``mixed_commands``: N accounts x M commands, shallow streams — the honest
  "normal" number.
- ``hot_account_withdraws``: one account with an ever-deepening stream. The
  no-overdraft rule replays the whole stream per withdraw, so this segment is
  expected to degrade with depth. Quantifying that degradation is the point:
  it is the bottleneck candidate for the Phase 3 write-up (fix: snapshots or
  a cached balance).

Honest scope: single process, single thread, SQLite on local disk, no HTTP,
no network. Numbers are a local baseline, not a service benchmark.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from cloudscale.adapters.sqlite_compat.dead_letter_store import (  # noqa: E402
    DeadLetteringProjectionStore,
)
from cloudscale.processes.resilient_consumer import ResilientConsumer  # noqa: E402
from cqrs import CommandHandler, SqliteEventStore  # noqa: E402


def _revision() -> str:
    try:
        return (
            subprocess.run(
                ("git", "rev-parse", "HEAD"),
                cwd=REPOSITORY_ROOT,
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            ).stdout.strip()
            or "unknown"
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def _percentile(sorted_values: list[float], fraction: float) -> float:
    """Nearest-rank percentile over an already-sorted, non-empty sample."""
    index = min(len(sorted_values) - 1, int(fraction * len(sorted_values)))
    return sorted_values[index]


def _summarize(latencies_ns: list[int], duration_seconds: float) -> dict:
    values_ms = sorted(ns / 1_000_000 for ns in latencies_ns)
    return {
        "operations": len(values_ms),
        "duration_seconds": round(duration_seconds, 3),
        "throughput_per_second": round(len(values_ms) / duration_seconds, 1),
        "latency_ms": {
            "p50": round(_percentile(values_ms, 0.50), 4),
            "p95": round(_percentile(values_ms, 0.95), 4),
            "p99": round(_percentile(values_ms, 0.99), 4),
            "max": round(values_ms[-1], 4),
            "mean": round(statistics.fmean(values_ms), 4),
        },
    }


def _timed(operation) -> int:
    started = time.perf_counter_ns()
    operation()
    return time.perf_counter_ns() - started


def run_load(accounts: int, commands_per_account: int, hot_withdraws: int) -> dict:
    with tempfile.TemporaryDirectory(prefix="cloudscale-load-") as workdir:
        store = SqliteEventStore(os.path.join(workdir, "events.db"))
        projection = DeadLetteringProjectionStore(
            path=os.path.join(workdir, "projection.db")
        )
        handler = CommandHandler(store)

        # -- segment 1: mixed commands over shallow streams ------------------
        mixed_latencies: list[int] = []
        segment_started = time.perf_counter()
        for account in range(accounts):
            account_id = f"acct-{account}"
            for step in range(commands_per_account):
                if step % 3 == 2:
                    command = {
                        "type": "Withdraw",
                        "account_id": account_id,
                        "amount": 1,
                    }
                else:
                    command = {
                        "type": "Deposit",
                        "account_id": account_id,
                        "amount": 10,
                    }
                mixed_latencies.append(_timed(lambda: handler.handle(command)))
        mixed_duration = time.perf_counter() - segment_started

        # -- segment 2: hot account, stream depth grows per withdraw ---------
        handler.handle(
            {"type": "Deposit", "account_id": "hot", "amount": hot_withdraws + 1}
        )
        hot_latencies: list[int] = []
        depth_samples: list[dict] = []
        segment_started = time.perf_counter()
        for step in range(hot_withdraws):
            elapsed = _timed(
                lambda: handler.handle(
                    {"type": "Withdraw", "account_id": "hot", "amount": 1}
                )
            )
            hot_latencies.append(elapsed)
            if step in (0, hot_withdraws // 2, hot_withdraws - 1):
                depth_samples.append(
                    {"stream_depth": step + 1, "latency_ms": elapsed / 1_000_000}
                )
        hot_duration = time.perf_counter() - segment_started

        # -- consumer catch-up ------------------------------------------------
        segment_started = time.perf_counter()
        report = ResilientConsumer(store, projection, batch=500).run()
        consumer_duration = time.perf_counter() - segment_started
        total_events = accounts * commands_per_account + hot_withdraws + 1
        assert report.applied == total_events, (
            f"consumer applied {report.applied}, expected {total_events}"
        )

        # -- query side --------------------------------------------------------
        query_latencies: list[int] = []
        segment_started = time.perf_counter()
        for account in range(accounts):
            account_id = f"acct-{account}"
            query_latencies.append(_timed(lambda: projection.balance(account_id)))
        query_duration = time.perf_counter() - segment_started

        projection.close()
        store.close()

    return {
        "schema_version": 1,
        "phase": 3,
        "harness": "scripts/load_and_observe.py",
        "revision": _revision(),
        "captured_at": datetime.now(UTC).isoformat(),
        "host": {
            "platform": platform.platform(),
            "python": platform.python_version(),
        },
        "workload": {
            "accounts": accounts,
            "commands_per_account": commands_per_account,
            "hot_withdraws": hot_withdraws,
            "storage": "sqlite file (temp dir), WAL",
        },
        "honest_scope": (
            "single process, single thread, local disk, no HTTP/network; "
            "local baseline only"
        ),
        "segments": {
            "mixed_commands": _summarize(mixed_latencies, mixed_duration),
            "hot_account_withdraws": {
                **_summarize(hot_latencies, hot_duration),
                "depth_samples": [
                    {
                        "stream_depth": sample["stream_depth"],
                        "latency_ms": round(sample["latency_ms"], 4),
                    }
                    for sample in depth_samples
                ],
                "expected_behavior": (
                    "latency grows with stream depth: the no-overdraft rule "
                    "replays the full stream per withdraw (O(n) hot path); "
                    "bottleneck candidate for the Phase 3 write-up"
                ),
            },
            "consumer_catchup": {
                "events_applied": report.applied,
                "duration_seconds": round(consumer_duration, 3),
                "throughput_per_second": round(report.applied / consumer_duration, 1),
            },
            "queries": _summarize(query_latencies, query_duration),
        },
    }


def _parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--accounts", type=int, default=200)
    parser.add_argument("--commands-per-account", type=int, default=50)
    parser.add_argument("--hot-withdraws", type=int, default=2000)
    parser.add_argument(
        "--evidence-root",
        type=Path,
        help="Override the default revision-bound evidence directory.",
    )
    return parser.parse_args(arguments)


def main(arguments: Sequence[str] | None = None) -> int:
    args = _parse_args(arguments)
    report = run_load(args.accounts, args.commands_per_account, args.hot_withdraws)

    evidence_directory = (
        args.evidence_root.resolve()
        if args.evidence_root is not None
        else REPOSITORY_ROOT / "evidence" / report["revision"] / "phase-3-load"
    )
    evidence_directory.mkdir(parents=True, exist_ok=True)
    report_path = evidence_directory / "report.json"
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    segments = report["segments"]
    print(f"revision: {report['revision']}")
    for name in ("mixed_commands", "hot_account_withdraws", "queries"):
        summary = segments[name]
        latency = summary["latency_ms"]
        print(
            f"{name}: {summary['throughput_per_second']}/s, "
            f"p50={latency['p50']}ms p95={latency['p95']}ms p99={latency['p99']}ms"
        )
    catchup = segments["consumer_catchup"]
    print(
        f"consumer_catchup: {catchup['events_applied']} events, "
        f"{catchup['throughput_per_second']}/s"
    )
    for sample in segments["hot_account_withdraws"]["depth_samples"]:
        print(f"hot withdraw @depth {sample['stream_depth']}: {sample['latency_ms']}ms")
    try:
        shown_path = report_path.relative_to(REPOSITORY_ROOT)
    except ValueError:
        shown_path = report_path
    print(f"report: {shown_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
