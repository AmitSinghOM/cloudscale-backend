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
import contextlib
import json
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
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


@contextlib.contextmanager
def _open_storage(storage: str, tracer):
    """Yield ``(store, projection, description, retryable_errors)`` for a backend.

    ``postgres`` provisions a throwaway database on the server at
    ``CLOUDSCALE_TEST_PG`` (default ``postgresql://localhost/postgres``) and
    DROPs it afterwards — nothing is left behind.
    """
    if storage == "postgres":
        import psycopg

        from cloudscale.adapters.postgres.event_store import PostgresEventStore
        from cloudscale.adapters.postgres.projection_store import (
            PostgresProjectionStore,
        )

        admin_dsn = os.environ.get(
            "CLOUDSCALE_TEST_PG", "postgresql://localhost/postgres"
        )
        database = f"cloudscale_load_{uuid.uuid4().hex[:12]}"
        admin = psycopg.connect(admin_dsn, autocommit=True)
        admin.execute(f'CREATE DATABASE "{database}"')
        dsn = f"{admin_dsn.rsplit('/', 1)[0]}/{database}"
        store = PostgresEventStore(dsn)
        projection = PostgresProjectionStore(dsn)
        try:
            yield (
                store,
                projection,
                "postgresql 17 (throwaway db, local server)",
                (psycopg.OperationalError,),
            )
        finally:
            store.close()
            projection.close()
            admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
            admin.close()
        return

    with tempfile.TemporaryDirectory(prefix="cloudscale-load-") as workdir:
        events_path = os.path.join(workdir, "events.db")
        projection_path = os.path.join(workdir, "projection.db")
        if tracer is not None:
            from cloudscale.adapters.telemetry import (
                TracedDeadLetteringProjectionStore,
                TracedSqliteEventStore,
            )

            store = TracedSqliteEventStore(events_path, tracer)
            projection = TracedDeadLetteringProjectionStore(projection_path, tracer)
        else:
            store = SqliteEventStore(events_path)
            projection = DeadLetteringProjectionStore(path=projection_path)
        yield (
            store,
            projection,
            "sqlite file (temp dir), WAL",
            None,
        )


def run_load(
    accounts: int,
    commands_per_account: int,
    hot_withdraws: int,
    trace_hot_path: bool = False,
    storage: str = "sqlite",
) -> dict:
    if storage not in ("sqlite", "postgres"):
        raise ValueError("storage must be 'sqlite' or 'postgres'")
    if trace_hot_path and storage != "sqlite":
        raise ValueError("--trace currently supports only the sqlite backend")

    tracer = None
    exporter = None
    if trace_hot_path:
        from cloudscale.adapters.telemetry import configure_in_memory_tracing

        tracer, exporter = configure_in_memory_tracing("cloudscale-load")

    with _open_storage(storage, tracer) as (
        store,
        projection,
        storage_description,
        retryable_errors,
    ):
        handler = CommandHandler(store)

        @contextlib.contextmanager
        def _segment_span(name: str):
            if tracer is None:
                yield
            else:
                with tracer.start_as_current_span(name):
                    yield

        def _handle_command(command: dict) -> None:
            if tracer is None:
                handler.handle(command)
            else:
                with tracer.start_as_current_span("command.handle"):
                    handler.handle(command)

        # -- segment 1: mixed commands over shallow streams ------------------
        mixed_latencies: list[int] = []
        segment_started = time.perf_counter()
        with _segment_span("segment.mixed_commands"):
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
                    mixed_latencies.append(_timed(lambda: _handle_command(command)))
        mixed_duration = time.perf_counter() - segment_started

        # -- segment 2: hot account, stream depth grows per withdraw ---------
        _handle_command(
            {"type": "Deposit", "account_id": "hot", "amount": hot_withdraws + 1}
        )
        hot_latencies: list[int] = []
        depth_samples: list[dict] = []
        segment_started = time.perf_counter()
        with _segment_span("segment.hot_withdraws"):
            for step in range(hot_withdraws):
                elapsed = _timed(
                    lambda: _handle_command(
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
        with _segment_span("segment.consumer_catchup"):
            consumer_kwargs: dict = {"batch": 500}
            if retryable_errors is not None:
                consumer_kwargs["retryable_errors"] = retryable_errors
            report = ResilientConsumer(store, projection, **consumer_kwargs).run()
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

    result = {
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
            "storage": storage_description,
            "traced": trace_hot_path,
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
                    "latency should stay flat in stream depth since the "
                    "withdraw guard memoizes its fold (perf fix d82039b); "
                    "regression here means the O(n) replay came back"
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
    if exporter is not None:
        from cloudscale.adapters.telemetry import summarize_spans

        result["trace"] = {
            "exporter": "in-memory (SimpleSpanProcessor)",
            "spans_by_name": summarize_spans(exporter),
        }
    return result


def _parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--accounts", type=int, default=200)
    parser.add_argument("--commands-per-account", type=int, default=50)
    parser.add_argument("--hot-withdraws", type=int, default=2000)
    parser.add_argument(
        "--storage",
        choices=("sqlite", "postgres"),
        default="sqlite",
        help="Storage backend. 'postgres' provisions and drops a throwaway "
        "database on the server at CLOUDSCALE_TEST_PG "
        "(default postgresql://localhost/postgres).",
    )
    parser.add_argument(
        "--trace",
        action="store_true",
        help="Trace the hot path with OpenTelemetry (in-memory exporter); "
        "adds overhead, so traced numbers are not comparable to untraced runs.",
    )
    parser.add_argument(
        "--evidence-root",
        type=Path,
        help="Override the default revision-bound evidence directory.",
    )
    return parser.parse_args(arguments)


def main(arguments: Sequence[str] | None = None) -> int:
    args = _parse_args(arguments)
    report = run_load(
        args.accounts,
        args.commands_per_account,
        args.hot_withdraws,
        trace_hot_path=args.trace,
        storage=args.storage,
    )

    evidence_directory = (
        args.evidence_root.resolve()
        if args.evidence_root is not None
        else REPOSITORY_ROOT / "evidence" / report["revision"] / "phase-3-load"
    )
    evidence_directory.mkdir(parents=True, exist_ok=True)
    if args.trace:
        report_name = "report-traced.json"
    elif args.storage == "postgres":
        report_name = "report-postgres.json"
    else:
        report_name = "report.json"
    report_path = evidence_directory / report_name
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
    if "trace" in report:
        print("spans (count, mean ms, p95 ms):")
        for name, stats in report["trace"]["spans_by_name"].items():
            print(
                f"  {name:<28} {stats['count']:>6}  "
                f"{stats['mean_ms']:>8}  {stats['p95_ms']:>8}"
            )
    try:
        shown_path = report_path.relative_to(REPOSITORY_ROOT)
    except ValueError:
        shown_path = report_path
    print(f"report: {shown_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
