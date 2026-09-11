#!/usr/bin/env python3
"""Drive the Milestone 1 network-scope gates against a real HTTP deployment.

Spawns a real uvicorn server (env-built app over the SQLite tier) and the
consumer loop as a separate process, then drives authenticated load over
localhost HTTP and evaluates the outstanding Milestone 1 gates honestly:

- sustained_1000_requests_per_second (combined, over the measured window)
- command_p99_at_most_300_ms
- query_p99_at_most_100_ms
- maximum_projection_lag_at_most_1_second (probe events, append->visible)

Writes revision-bound evidence under ``evidence/<sha>/phase-4-http-gates/``
with per-gate pass/fail. The availability gate (99.9% over 30 days) is out
of scope for a bench run and recorded as not_evaluated.

Honest scope: localhost loopback, one uvicorn worker, SQLite storage tier,
GIL-threaded Python load generator on the same host. Numbers are a local
deployment baseline, not a service benchmark.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Sequence
from uuid import uuid4

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

import httpx  # noqa: E402
import jwt  # noqa: E402

# Bench-only credential: generated tokens live only for one localhost
# gate run against a throwaway deployment. Not a real secret.
BENCH_ONLY_SECRET = "http-gate-run-secret-0123456789abcdef-0123456789abcdef"

GATE_RPS = 1000.0
GATE_COMMAND_P99_MS = 300.0
GATE_QUERY_P99_MS = 100.0
GATE_MAX_LAG_SECONDS = 1.0


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


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _token() -> str:
    return jwt.encode(
        {
            "iss": "cloudscale",
            "sub": "gate-runner",
            "scope": "accounts:admin",
            "jti": "gate-run-" + uuid4().hex,
            "iat": datetime.now(UTC),
            "exp": datetime.now(UTC) + timedelta(hours=1),
        },
        BENCH_ONLY_SECRET,
        algorithm="HS256",
    )


def _percentile(sorted_values: list[float], fraction: float) -> float:
    index = min(len(sorted_values) - 1, int(fraction * len(sorted_values)))
    return sorted_values[index]


def _summary(latencies_ms: list[float]) -> dict:
    values = sorted(latencies_ms)
    return {
        "operations": len(values),
        "p50_ms": round(_percentile(values, 0.50), 3),
        "p95_ms": round(_percentile(values, 0.95), 3),
        "p99_ms": round(_percentile(values, 0.99), 3),
        "max_ms": round(values[-1], 3),
        "mean_ms": round(statistics.fmean(values), 3),
    }


class _CommandWorker(threading.Thread):
    """Posts deposits to its own account, tracking expected_version."""

    def __init__(self, base_url: str, headers: dict, stop: threading.Event) -> None:
        super().__init__(daemon=True)
        self.account = f"gate-cmd-{uuid4().hex[:10]}"
        self.latencies_ms: list[float] = []
        self.errors = 0
        self._url = f"{base_url}/v1/accounts/{self.account}/commands"
        self._headers = headers
        self._halt = stop
        self._version = 0

    def run(self) -> None:
        with httpx.Client(timeout=10.0) as client:
            while not self._halt.is_set():
                body = {
                    "command_id": str(uuid4()),
                    "type": "deposit",
                    "amount": 10,
                    "expected_version": self._version,
                }
                started = time.perf_counter()
                try:
                    response = client.post(self._url, json=body, headers=self._headers)
                except httpx.HTTPError:
                    self.errors += 1
                    continue
                self.latencies_ms.append((time.perf_counter() - started) * 1000)
                if response.status_code == 201:
                    self._version = int(response.json()["committed_version"])
                elif response.status_code == 409:
                    self._version = int(response.json()["current_version"])
                else:
                    self.errors += 1


class _QueryWorker(threading.Thread):
    """Reads a fixed account's balance in a tight loop."""

    def __init__(
        self, base_url: str, headers: dict, account: str, stop: threading.Event
    ) -> None:
        super().__init__(daemon=True)
        self.latencies_ms: list[float] = []
        self.errors = 0
        self._url = f"{base_url}/v1/accounts/{account}/balance"
        self._headers = headers
        self._halt = stop

    def run(self) -> None:
        with httpx.Client(timeout=10.0) as client:
            while not self._halt.is_set():
                started = time.perf_counter()
                try:
                    response = client.get(self._url, headers=self._headers)
                except httpx.HTTPError:
                    self.errors += 1
                    continue
                self.latencies_ms.append((time.perf_counter() - started) * 1000)
                if response.status_code not in (200, 404):
                    self.errors += 1


class _LagProber(threading.Thread):
    """Measures append->queryable projection lag with probe deposits."""

    def __init__(self, base_url: str, headers: dict, stop: threading.Event) -> None:
        super().__init__(daemon=True)
        self.lags_seconds: list[float] = []
        self.failures = 0
        self._base_url = base_url
        self._headers = headers
        self._halt = stop
        self._account = f"gate-probe-{uuid4().hex[:10]}"
        self._version = 0

    def run(self) -> None:
        commands_url = f"{self._base_url}/v1/accounts/{self._account}/commands"
        balance_url = f"{self._base_url}/v1/accounts/{self._account}/balance"
        with httpx.Client(timeout=10.0) as client:
            while not self._halt.is_set():
                body = {
                    "command_id": str(uuid4()),
                    "type": "deposit",
                    "amount": 1,
                    "expected_version": self._version,
                }
                appended_at = time.perf_counter()
                response = client.post(commands_url, json=body, headers=self._headers)
                if response.status_code != 201:
                    self.failures += 1
                    time.sleep(0.5)
                    continue
                committed = int(response.json()["committed_version"])
                self._version = committed
                deadline = time.monotonic() + 5.0
                while time.monotonic() < deadline:
                    balance = client.get(balance_url, headers=self._headers)
                    if (
                        balance.status_code == 200
                        and int(balance.json()["version"]) >= committed
                    ):
                        self.lags_seconds.append(time.perf_counter() - appended_at)
                        break
                    time.sleep(0.005)
                else:
                    self.failures += 1
                time.sleep(0.5)


def run_gates(
    duration_seconds: float,
    command_workers: int,
    query_workers: int,
    storage: str = "sqlite",
) -> dict:
    if storage not in ("sqlite", "postgres"):
        raise ValueError("storage must be 'sqlite' or 'postgres'")
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    headers = {"Authorization": f"Bearer {_token()}"}

    admin = None
    database = None
    admin_dsn = os.environ.get("CLOUDSCALE_TEST_PG", "postgresql://localhost/postgres")
    with tempfile.TemporaryDirectory(prefix="cloudscale-gate-") as workdir:
        env = dict(
            os.environ,
            CLOUDSCALE_JWT_SECRET=BENCH_ONLY_SECRET,
            CLOUDSCALE_STORAGE=storage,
            # Bench-only: the per-subject limiter would throttle a single
            # load-generating subject; disabling it is recorded in the report.
            CLOUDSCALE_RATE_LIMIT_PER_MINUTE="0",
            CLOUDSCALE_LOG_DB=os.path.join(workdir, "log.db"),
            CLOUDSCALE_PROJECTION_DB=os.path.join(workdir, "projection.db"),
        )
        # One try/finally covers throwaway-DB creation AND process spawns, so
        # a failure at any point tears down whatever already exists.
        processes: list[subprocess.Popen] = []
        try:
            if storage == "postgres":
                import psycopg

                database = f"cloudscale_gate_{uuid4().hex[:12]}"
                admin = psycopg.connect(admin_dsn, autocommit=True)
                admin.execute(f'CREATE DATABASE "{database}"')
                env["CLOUDSCALE_PG_DSN"] = f"{admin_dsn.rsplit('/', 1)[0]}/{database}"
            server = subprocess.Popen(
                (
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "--factory",
                    "cloudscale.entrypoints.http.main:build_app",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--log-level",
                    "warning",
                ),
                cwd=REPOSITORY_ROOT,
                env=env,
            )
            processes.append(server)
            consumer = subprocess.Popen(
                (sys.executable, "-m", "cloudscale.entrypoints.consumer_loop"),
                cwd=REPOSITORY_ROOT,
                env=env,
            )
            processes.append(consumer)
            _wait_for_health(base_url)

            stop = threading.Event()
            commands = [
                _CommandWorker(base_url, headers, stop) for _ in range(command_workers)
            ]
            queries = [
                _QueryWorker(base_url, headers, commands[0].account, stop)
                for _ in range(query_workers)
            ]
            prober = _LagProber(base_url, headers, stop)

            for worker in (*commands, *queries, prober):
                worker.start()
            time.sleep(2.0)  # warmup, excluded by clearing samples
            for worker in (*commands, *queries):
                worker.latencies_ms.clear()

            measured_started = time.perf_counter()
            time.sleep(duration_seconds)
            stop.set()
            for worker in (*commands, *queries, prober):
                worker.join(timeout=15.0)
            measured_duration = time.perf_counter() - measured_started
        finally:
            for process in reversed(processes):
                process.terminate()
            for process in reversed(processes):
                try:
                    process.wait(timeout=10.0)
                except subprocess.TimeoutExpired:
                    process.kill()
            if admin is not None and database is not None:
                # Cleanup must never mask the run's real error: IF EXISTS
                # covers a CREATE that failed midway, and close() always runs.
                try:
                    admin.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
                finally:
                    admin.close()

    command_latencies = [value for worker in commands for value in worker.latencies_ms]
    query_latencies = [value for worker in queries for value in worker.latencies_ms]
    total_requests = len(command_latencies) + len(query_latencies)
    achieved_rps = total_requests / measured_duration
    command_summary = _summary(command_latencies)
    query_summary = _summary(query_latencies)
    max_lag = max(prober.lags_seconds) if prober.lags_seconds else None
    errors = sum(worker.errors for worker in (*commands, *queries))

    gates = {
        "sustained_1000_requests_per_second": {
            "threshold": GATE_RPS,
            "observed": round(achieved_rps, 1),
            "passed": achieved_rps >= GATE_RPS,
        },
        "command_p99_at_most_300_ms": {
            "threshold": GATE_COMMAND_P99_MS,
            "observed": command_summary["p99_ms"],
            "passed": command_summary["p99_ms"] <= GATE_COMMAND_P99_MS,
        },
        "query_p99_at_most_100_ms": {
            "threshold": GATE_QUERY_P99_MS,
            "observed": query_summary["p99_ms"],
            "passed": query_summary["p99_ms"] <= GATE_QUERY_P99_MS,
        },
        "maximum_projection_lag_at_most_1_second": {
            "threshold": GATE_MAX_LAG_SECONDS,
            "observed": round(max_lag, 4) if max_lag is not None else None,
            "passed": max_lag is not None
            and prober.failures == 0
            and max_lag <= GATE_MAX_LAG_SECONDS,
        },
        "availability_at_least_99_9_percent_over_30_days": {
            "threshold": None,
            "observed": None,
            "passed": None,
            "note": "not_evaluated: requires 30 days of operation, not a bench run",
        },
    }

    return {
        "schema_version": 1,
        "phase": 4,
        "harness": "scripts/http_gate_run.py",
        "revision": _revision(),
        "captured_at": datetime.now(UTC).isoformat(),
        "deployment": {
            "server": "uvicorn, 1 worker, 127.0.0.1 loopback",
            "consumer": "separate process (cloudscale.entrypoints.consumer_loop)",
            "storage": (
                "postgresql 17 (throwaway db, local server)"
                if storage == "postgres"
                else "sqlite tier (temp dir)"
            ),
            "auth": "JWT bearer on every request (accounts:admin scope)",
            "rate_limit": "disabled for bench (CLOUDSCALE_RATE_LIMIT_PER_MINUTE=0)",
        },
        "workload": {
            "duration_seconds": round(measured_duration, 2),
            "command_workers": command_workers,
            "query_workers": query_workers,
            "request_errors": errors,
            "lag_probe_failures": prober.failures,
            "lag_probes": len(prober.lags_seconds),
        },
        "honest_scope": (
            "localhost loopback, single uvicorn worker, SQLite tier, "
            "GIL-threaded load generator sharing the host; a local deployment "
            "baseline, not a service benchmark"
        ),
        "results": {
            "achieved_rps": round(achieved_rps, 1),
            "commands": command_summary,
            "queries": query_summary,
            "projection_lag_seconds": {
                "max": round(max_lag, 4) if max_lag is not None else None,
                "samples": [round(v, 4) for v in prober.lags_seconds],
            },
        },
        "gates": gates,
        "gates_passed": sum(1 for gate in gates.values() if gate["passed"] is True),
        "gates_failed": sum(1 for gate in gates.values() if gate["passed"] is False),
    }


def _wait_for_health(base_url: str, timeout_seconds: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"{base_url}/v1/health", timeout=2.0).status_code == 200:
                return
        except httpx.HTTPError as error:
            last_error = error
        time.sleep(0.2)
    raise RuntimeError(f"server never became healthy: {last_error}")


def _parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, default=10.0)
    parser.add_argument(
        "--storage",
        choices=("sqlite", "postgres"),
        default="sqlite",
        help="Storage tier for the deployment under test. 'postgres' "
        "provisions and drops a throwaway database at CLOUDSCALE_TEST_PG.",
    )
    parser.add_argument("--command-workers", type=int, default=4)
    parser.add_argument("--query-workers", type=int, default=8)
    parser.add_argument("--evidence-root", type=Path)
    return parser.parse_args(arguments)


def main(arguments: Sequence[str] | None = None) -> int:
    args = _parse_args(arguments)
    report = run_gates(
        args.duration, args.command_workers, args.query_workers, storage=args.storage
    )

    evidence_directory = (
        args.evidence_root.resolve()
        if args.evidence_root is not None
        else REPOSITORY_ROOT / "evidence" / report["revision"] / "phase-4-http-gates"
    )
    evidence_directory.mkdir(parents=True, exist_ok=True)
    report_name = (
        "report-postgres.json" if args.storage == "postgres" else "report.json"
    )
    report_path = evidence_directory / report_name
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    print(f"revision: {report['revision']}")
    print(f"achieved_rps: {report['results']['achieved_rps']}")
    print(f"commands: {report['results']['commands']}")
    print(f"queries: {report['results']['queries']}")
    print(f"projection lag max: {report['results']['projection_lag_seconds']['max']}s")
    for name, gate in report["gates"].items():
        status = (
            "PASS"
            if gate["passed"] is True
            else "FAIL"
            if gate["passed"] is False
            else "N/A "
        )
        print(f"  {status} {name}: observed={gate['observed']}")
    try:
        shown = report_path.relative_to(REPOSITORY_ROOT)
    except ValueError:
        shown = report_path
    print(f"report: {shown}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
