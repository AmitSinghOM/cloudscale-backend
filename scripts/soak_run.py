"""Soak run: hold moderate load for a long time and watch what DRIFTS.

The 10-second gate (``http_gate_run.py``) proves the hot path is fast at
steady state. This proves the service stays correct and fast for hours:
memory, latency tails, projection lag, connections, table growth, error
rates. The interesting output is the slope, not the mean.

Topology (all real processes, PostgreSQL tier, throwaway database):
server (1 uvicorn worker) · two consumers (one leader, one standby) ·
retention subprocess on a schedule · this process as load generator and
sampler.

Load rotates across many accounts on purpose: the per-account memoized
fold state and the per-key limiter buckets only grow when keys are
distinct, so a single-account soak would hide exactly the leaks sought.

Injected event: at ``--failover-at`` seconds the leader consumer is
SIGKILLed; the standby must take over and lag must return to zero.

Pass criteria are fixed BEFORE the run (see ``evaluate``). The report is
written regardless; commit it only when every criterion passes, same rule
as the gates. Run detached for anything longer than a few minutes::

    nohup .venv/bin/python scripts/soak_run.py --duration 7200 \\
        --failover-at 3600 > soak.log 2>&1 &
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import signal
import statistics
import subprocess
import sys
import threading
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

import httpx  # noqa: E402

from scripts.http_gate_run import (  # noqa: E402
    BENCH_ONLY_SECRET,
    _free_port,
    _percentile,
    _revision,
    _token,
    _wait_for_health,
)

# -- pass criteria (fixed before the run) --------------------------------------------

MAX_RSS_SLOPE_MB_PER_MIN = 1.0  # after warm-up, per process
MAX_COMMAND_P99_MS = 300.0  # every window
MAX_QUERY_P99_MS = 100.0  # every window
MAX_LAG_SECONDS = 1.0  # every window
MAX_FAILOVER_SECONDS = 5.0
MAX_PG_CONNECTION_SPREAD = 2  # max - min across samples
WARMUP_SECONDS = 600.0  # RSS slope measured after this

GROWTH_TABLES = ("command_results", "rate_limit_buckets", "outbox", "events")


# -- load ---------------------------------------------------------------------------


class _Pacer:
    """Sleep-based pacing so a worker issues ~``rps`` requests per second."""

    def __init__(self, rps: float) -> None:
        self._interval = 1.0 / rps if rps > 0 else 0.0
        self._next = time.perf_counter()

    def wait(self) -> None:
        if self._interval == 0.0:
            return
        self._next += self._interval
        delay = self._next - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        else:  # fell behind: don't accumulate debt
            self._next = time.perf_counter()


class _Windows:
    """Thread-safe latency/status collection drained per sample window."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.command_ms: list[float] = []
        self.query_ms: list[float] = []
        self.statuses: dict[int, int] = {}
        self.transport_errors = 0

    def record(self, kind: str, ms: float, status: int) -> None:
        with self._lock:
            (self.command_ms if kind == "command" else self.query_ms).append(ms)
            self.statuses[status] = self.statuses.get(status, 0) + 1

    def transport_error(self) -> None:
        with self._lock:
            self.transport_errors += 1

    def drain(self) -> dict:
        with self._lock:
            out = {
                "command_ms": self.command_ms,
                "query_ms": self.query_ms,
                "statuses": self.statuses,
                "transport_errors": self.transport_errors,
            }
            self.command_ms, self.query_ms = [], []
            self.statuses, self.transport_errors = {}, 0
            return out


class _CommandWorker(threading.Thread):
    """Deposits into a rotating shard of accounts, tracking each version."""

    def __init__(
        self,
        base_url: str,
        headers: dict,
        accounts: Sequence[str],
        rps: float,
        windows: _Windows,
        stop: threading.Event,
    ) -> None:
        super().__init__(daemon=True)
        self._base = base_url
        self._headers = headers
        self._accounts = list(accounts)
        self._versions: dict[str, int] = {}
        self._pacer = _Pacer(rps)
        self._windows = windows
        self._halt = stop
        self._rng = random.Random(hash(tuple(accounts)))  # noqa: S311 (load shaping)

    def run(self) -> None:
        with httpx.Client(timeout=10.0) as client:
            while not self._halt.is_set():
                self._pacer.wait()
                account = self._rng.choice(self._accounts)
                body = {
                    "command_id": str(uuid4()),
                    "type": "deposit",
                    "amount": 10,
                    "expected_version": self._versions.get(account, 0),
                }
                started = time.perf_counter()
                try:
                    response = client.post(
                        f"{self._base}/v1/accounts/{account}/commands",
                        json=body,
                        headers=self._headers,
                    )
                except httpx.HTTPError:
                    self._windows.transport_error()
                    continue
                elapsed = (time.perf_counter() - started) * 1000
                self._windows.record("command", elapsed, response.status_code)
                if response.status_code == 201:
                    self._versions[account] = int(response.json()["committed_version"])
                elif response.status_code == 409:
                    self._versions[account] = int(response.json()["current_version"])


class _QueryWorker(threading.Thread):
    def __init__(
        self,
        base_url: str,
        headers: dict,
        accounts: Sequence[str],
        rps: float,
        windows: _Windows,
        stop: threading.Event,
    ) -> None:
        super().__init__(daemon=True)
        self._base = base_url
        self._headers = headers
        self._accounts = list(accounts)
        self._pacer = _Pacer(rps)
        self._windows = windows
        self._halt = stop
        self._rng = random.Random(len(accounts))  # noqa: S311 (load shaping)

    def run(self) -> None:
        with httpx.Client(timeout=10.0) as client:
            while not self._halt.is_set():
                self._pacer.wait()
                account = self._rng.choice(self._accounts)
                started = time.perf_counter()
                try:
                    response = client.get(
                        f"{self._base}/v1/accounts/{account}/balance",
                        headers=self._headers,
                    )
                except httpx.HTTPError:
                    self._windows.transport_error()
                    continue
                elapsed = (time.perf_counter() - started) * 1000
                self._windows.record("query", elapsed, response.status_code)


# -- sampling -----------------------------------------------------------------------


def _rss_mb(pid: int) -> float | None:
    try:
        out = subprocess.run(
            ("ps", "-o", "rss=", "-p", str(pid)),
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
        return round(int(out) / 1024, 2) if out else None
    except (OSError, subprocess.SubprocessError, ValueError):
        return None


def _fd_count(pid: int) -> int | None:
    proc_fd = Path(f"/proc/{pid}/fd")  # Linux only; lsof is too slow to sample
    if proc_fd.is_dir():
        try:
            return len(list(proc_fd.iterdir()))
        except OSError:
            return None
    return None


def _scrape(port: int) -> dict[str, float]:
    """Flatten a Prometheus exposition into {name{labels}: value}."""
    values: dict[str, float] = {}
    try:
        text = httpx.get(f"http://127.0.0.1:{port}/metrics", timeout=2.0).text
    except httpx.HTTPError:
        return values
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        name, _, value = line.rpartition(" ")
        try:
            values[name] = float(value)
        except ValueError:
            continue
    return values


def _metric(values: dict[str, float], prefix: str) -> float | None:
    for key, value in values.items():
        if key.startswith(prefix):
            return value
    return None


def _pg_sample(dsn: str) -> dict:
    import psycopg

    def scalar(conn: object, sql: str, params: tuple = ()) -> object:
        row = conn.execute(sql, params).fetchone()  # type: ignore[attr-defined]
        return row[0] if row is not None else None

    with psycopg.connect(dsn, connect_timeout=3) as conn:
        connections = scalar(
            conn,
            "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database()",
        )
        # to_regclass() is NULL for a table this deployment did not create
        # (e.g. rate_limit_buckets without the postgres limiter backend).
        sizes = {
            table: scalar(
                conn,
                "SELECT pg_total_relation_size(to_regclass(%s))",
                (table,),
            )
            for table in GROWTH_TABLES
        }
        rows = scalar(conn, "SELECT COUNT(*) FROM command_results")
    return {"pg_connections": connections, "sizes": sizes, "command_results_rows": rows}


# -- evaluation ---------------------------------------------------------------------


def _slope_per_min(points: list[tuple[float, float]]) -> float | None:
    """Least-squares slope of (t_seconds, value) in value/minute."""
    if len(points) < 3:
        return None
    xs = [p[0] / 60.0 for p in points]
    ys = [p[1] for p in points]
    mean_x, mean_y = statistics.fmean(xs), statistics.fmean(ys)
    var = sum((x - mean_x) ** 2 for x in xs)
    if var == 0:
        return None
    return sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=True)) / var


def evaluate(samples: list[dict], failover: dict | None, duration: float) -> dict:
    checks: dict[str, dict] = {}

    def check(name: str, passed: bool, observed: object, limit: object) -> None:
        checks[name] = {"pass": bool(passed), "observed": observed, "limit": limit}

    steady = [s for s in samples if s["t"] >= WARMUP_SECONDS]
    for proc in ("server", "consumer_a", "consumer_b"):
        pts = [(s["t"], s[f"{proc}_rss_mb"]) for s in steady if s.get(f"{proc}_rss_mb")]
        slope = _slope_per_min(pts)
        if slope is None:
            check(f"rss_slope_{proc}", duration < WARMUP_SECONDS, "insufficient", "n/a")
        else:
            check(
                f"rss_slope_{proc}_mb_per_min",
                slope <= MAX_RSS_SLOPE_MB_PER_MIN,
                round(slope, 3),
                MAX_RSS_SLOPE_MB_PER_MIN,
            )

    cmd_p99s = [s["command_p99_ms"] for s in samples if s.get("command_p99_ms")]
    qry_p99s = [s["query_p99_ms"] for s in samples if s.get("query_p99_ms")]
    check(
        "command_p99_every_window",
        all(p <= MAX_COMMAND_P99_MS for p in cmd_p99s),
        round(max(cmd_p99s), 2) if cmd_p99s else None,
        MAX_COMMAND_P99_MS,
    )
    check(
        "query_p99_every_window",
        all(p <= MAX_QUERY_P99_MS for p in qry_p99s),
        round(max(qry_p99s), 2) if qry_p99s else None,
        MAX_QUERY_P99_MS,
    )

    lags = [s["lag_seconds"] for s in samples if s.get("lag_seconds") is not None]
    # Exclude the failover window itself: lag legitimately rises until the standby leads.
    if failover:
        lags = [
            s["lag_seconds"]
            for s in samples
            if s.get("lag_seconds") is not None
            and not (
                failover["t"] <= s["t"] <= failover["t"] + MAX_FAILOVER_SECONDS + 30
            )
        ]
    check(
        "lag_every_window_s",
        all(v <= MAX_LAG_SECONDS for v in lags),
        max(lags) if lags else None,
        MAX_LAG_SECONDS,
    )

    five_xx = sum(s.get("status_5xx", 0) for s in samples)
    check("zero_5xx", five_xx == 0, five_xx, 0)
    transport = sum(s.get("transport_errors", 0) for s in samples)
    check("zero_transport_errors", transport == 0, transport, 0)
    dead = max((s.get("dead_lettered_total") or 0) for s in samples) if samples else 0
    check("zero_dead_letters", dead == 0, dead, 0)

    # Connection count legitimately steps down when the leader is killed (its
    # lease + pool connections go with it), so measure the spread per phase.
    def phase_spread(rows: list[dict]) -> int | None:
        values = [
            s["pg_connections"] for s in rows if s.get("pg_connections") is not None
        ]
        return (max(values) - min(values)) if values else None

    if failover:
        before = [s for s in samples if s["t"] < failover["t"]]
        after = [s for s in samples if s["t"] > failover["t"] + 5]
        spreads = [
            x for x in (phase_spread(before), phase_spread(after)) if x is not None
        ]
        spread = max(spreads) if spreads else None
    else:
        spread = phase_spread(samples)
    check(
        "pg_connection_spread",
        spread is not None and spread <= MAX_PG_CONNECTION_SPREAD,
        spread,
        MAX_PG_CONNECTION_SPREAD,
    )

    rows = [
        s["command_results_rows"]
        for s in samples
        if s.get("command_results_rows") is not None
    ]
    decreased = any(b < a for a, b in zip(rows, rows[1:], strict=False))
    check("retention_sawtooth_observed", decreased, decreased, True)

    if failover:
        check(
            "failover_seconds",
            failover.get("seconds") is not None
            and failover["seconds"] <= MAX_FAILOVER_SECONDS,
            failover.get("seconds"),
            MAX_FAILOVER_SECONDS,
        )

    return {"pass": all(c["pass"] for c in checks.values()), "checks": checks}


# -- orchestration ------------------------------------------------------------------


def run_soak(args: argparse.Namespace) -> dict:
    import psycopg

    admin_dsn = os.environ.get("CLOUDSCALE_TEST_PG", "postgresql://localhost/postgres")
    database = f"cloudscale_soak_{uuid4().hex[:12]}"
    admin = psycopg.connect(admin_dsn, autocommit=True)
    admin.execute(f'CREATE DATABASE "{database}"')
    dsn = f"{admin_dsn.rsplit('/', 1)[0]}/{database}"

    port, metrics_a, metrics_b = _free_port(), _free_port(), _free_port()
    base_url = f"http://127.0.0.1:{port}"
    headers = {"Authorization": f"Bearer {_token()}"}
    env = dict(
        os.environ,
        CLOUDSCALE_JWT_SECRET=BENCH_ONLY_SECRET,
        CLOUDSCALE_STORAGE="postgres",
        CLOUDSCALE_PG_DSN=dsn,
        CLOUDSCALE_RATE_LIMIT_PER_MINUTE="0",  # single bench subject; recorded
        CLOUDSCALE_CLIENT_RATE_LIMIT_PER_MINUTE="0",  # single bench client; recorded
    )
    procs: dict[str, subprocess.Popen] = {}
    samples: list[dict] = []
    retention_runs: list[dict] = []
    failover: dict | None = None
    stop = threading.Event()
    windows = _Windows()
    accounts = [f"soak-{i:06d}" for i in range(args.accounts)]
    started_at = time.monotonic()

    def spawn(name: str, cmd: tuple[str, ...], extra: dict[str, str]) -> None:
        procs[name] = subprocess.Popen(cmd, cwd=REPOSITORY_ROOT, env={**env, **extra})

    try:
        spawn(
            "server",
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
            {},
        )
        consumer_cmd = (sys.executable, "-m", "cloudscale.entrypoints.consumer_loop")
        spawn(
            "consumer_a",
            consumer_cmd,
            {"CLOUDSCALE_CONSUMER_METRICS_PORT": str(metrics_a)},
        )
        spawn(
            "consumer_b",
            consumer_cmd,
            {"CLOUDSCALE_CONSUMER_METRICS_PORT": str(metrics_b)},
        )
        _wait_for_health(base_url)
        time.sleep(1.0)  # let one consumer take the lease

        shard = max(1, args.accounts // args.command_workers)
        workers: list[threading.Thread] = []
        for i in range(args.command_workers):
            workers.append(
                _CommandWorker(
                    base_url,
                    headers,
                    accounts[i * shard : (i + 1) * shard],
                    args.command_rps / args.command_workers,
                    windows,
                    stop,
                )
            )
        for _ in range(args.query_workers):
            workers.append(
                _QueryWorker(
                    base_url,
                    headers,
                    accounts,
                    args.query_rps / args.query_workers,
                    windows,
                    stop,
                )
            )
        for worker in workers:
            worker.start()

        next_sample = time.monotonic() + args.sample_interval
        next_retention = time.monotonic() + args.retention_interval
        failover_done = args.failover_at is None
        consumer_ports = {"consumer_a": metrics_a, "consumer_b": metrics_b}

        while (elapsed := time.monotonic() - started_at) < args.duration:
            now = time.monotonic()
            if now >= next_retention:
                retention_run = subprocess.run(
                    (
                        sys.executable,
                        "-m",
                        "cloudscale.entrypoints.retention",
                        "--command-results-days",
                        str(args.retention_window_seconds / 86400),
                        "--rate-limit-idle-seconds",
                        "60",
                    ),
                    cwd=REPOSITORY_ROOT,
                    env=env,
                    check=False,
                    timeout=120,
                    capture_output=True,
                )
                retention_runs.append(
                    {
                        "t": round(elapsed, 1),
                        "exit": retention_run.returncode,
                        "report": retention_run.stdout.decode(errors="replace").strip()[
                            -300:
                        ],
                    }
                )
                next_retention = now + args.retention_interval
            if not failover_done and elapsed >= args.failover_at:
                leader = next(
                    (
                        n
                        for n, p in consumer_ports.items()
                        if _metric(_scrape(p), "cloudscale_consumer_is_leader") == 1.0
                    ),
                    None,
                )
                if leader:
                    procs[leader].send_signal(signal.SIGKILL)
                    procs[leader].wait(timeout=10)
                    kill_t = time.monotonic()
                    standby = "consumer_b" if leader == "consumer_a" else "consumer_a"
                    took = None
                    while time.monotonic() - kill_t < 60:
                        m = _scrape(consumer_ports[standby])
                        if (
                            _metric(m, "cloudscale_consumer_is_leader") == 1.0
                            and (_metric(m, "cloudscale_consumer_lag_events") or 0) == 0
                        ):
                            took = round(time.monotonic() - kill_t, 2)
                            break
                        time.sleep(0.25)
                    failover = {
                        "t": round(elapsed, 1),
                        "killed": leader,
                        "seconds": took,
                    }
                    del consumer_ports[leader]
                failover_done = True
            if now >= next_sample:
                window = windows.drain()
                sample: dict = {"t": round(elapsed, 1)}
                for name, proc in procs.items():
                    if proc.poll() is None:
                        sample[f"{name}_rss_mb"] = _rss_mb(proc.pid)
                        sample[f"{name}_fds"] = _fd_count(proc.pid)
                cmd = sorted(window["command_ms"])
                qry = sorted(window["query_ms"])
                sample["command_ops"] = len(cmd)
                sample["query_ops"] = len(qry)
                sample["command_p99_ms"] = (
                    round(_percentile(cmd, 0.99), 2) if cmd else None
                )
                sample["query_p99_ms"] = (
                    round(_percentile(qry, 0.99), 2) if qry else None
                )
                sample["status_5xx"] = sum(
                    n for s, n in window["statuses"].items() if 500 <= s <= 599
                )
                sample["status_4xx_non_409"] = sum(
                    n
                    for s, n in window["statuses"].items()
                    if 400 <= s <= 499 and s != 409
                )
                sample["transport_errors"] = window["transport_errors"]
                leader_metrics = next(
                    (
                        m
                        for p in consumer_ports.values()
                        if (m := _scrape(p))
                        and _metric(m, "cloudscale_consumer_is_leader") == 1.0
                    ),
                    {},
                )
                lag_events = _metric(leader_metrics, "cloudscale_consumer_lag_events")
                last_drain = _metric(
                    leader_metrics, "cloudscale_consumer_last_drain_timestamp"
                )
                sample["lag_events"] = lag_events
                sample["lag_seconds"] = (
                    round(max(0.0, time.time() - last_drain), 3) if last_drain else None
                )
                sample["dead_lettered_total"] = _metric(
                    leader_metrics, "cloudscale_consumer_events_dead_lettered_total"
                )
                try:
                    sample.update(_pg_sample(dsn))
                except Exception as exc:  # noqa: BLE001 — sampler must not die
                    sample["pg_error"] = type(exc).__name__
                samples.append(sample)
                print(json.dumps(sample), flush=True)
                next_sample = now + args.sample_interval
            time.sleep(0.2)
    finally:
        stop.set()
        for proc in procs.values():
            if proc.poll() is None:
                proc.terminate()
        for proc in procs.values():
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        try:
            admin.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
        finally:
            admin.close()

    verdict = evaluate(samples, failover, args.duration)
    return {
        "kind": "soak",
        "harness": "scripts/soak_run.py",
        "revision": _revision(),
        "captured_at": datetime.now(UTC).isoformat(),
        "deployment": {
            "server": "uvicorn, 1 worker, 127.0.0.1 loopback",
            "consumers": 2,
            "storage": "postgresql (throwaway db, local server)",
            "rate_limits": "disabled (single bench subject/client); recorded",
        },
        "load": {
            "duration_seconds": args.duration,
            "command_rps_target": args.command_rps,
            "query_rps_target": args.query_rps,
            "accounts": args.accounts,
            "sample_interval_seconds": args.sample_interval,
            "retention_interval_seconds": args.retention_interval,
            "retention_window_seconds": args.retention_window_seconds,
        },
        "failover": failover,
        "retention_runs": retention_runs,
        "criteria": {
            "max_rss_slope_mb_per_min": MAX_RSS_SLOPE_MB_PER_MIN,
            "max_command_p99_ms": MAX_COMMAND_P99_MS,
            "max_query_p99_ms": MAX_QUERY_P99_MS,
            "max_lag_seconds": MAX_LAG_SECONDS,
            "max_failover_seconds": MAX_FAILOVER_SECONDS,
            "warmup_seconds": WARMUP_SECONDS,
        },
        "verdict": verdict,
        "samples": samples,
    }


def _write(report: dict, out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "report.json"
    csv_path = out_dir / "samples.csv"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    keys: list[str] = []
    for s in report["samples"]:
        for k in s:
            if k not in keys:
                keys.append(k)
    with csv_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=keys)
        writer.writeheader()
        for s in report["samples"]:
            writer.writerow(
                {k: (json.dumps(v) if isinstance(v, dict) else v) for k, v in s.items()}
            )
    return report_path, csv_path


def _parse(arguments: Sequence[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--duration", type=float, default=7200.0, help="seconds")
    p.add_argument("--command-rps", type=float, default=100.0)
    p.add_argument("--query-rps", type=float, default=200.0)
    p.add_argument("--command-workers", type=int, default=4)
    p.add_argument("--query-workers", type=int, default=4)
    p.add_argument("--accounts", type=int, default=50_000)
    p.add_argument("--sample-interval", type=float, default=30.0)
    p.add_argument("--retention-interval", type=float, default=600.0)
    p.add_argument(
        "--retention-window-seconds",
        type=float,
        default=300.0,
        help="idempotency window used for the soak (short, to observe the sawtooth)",
    )
    p.add_argument(
        "--failover-at", type=float, default=None, help="seconds; omit to skip"
    )
    p.add_argument("--out", type=Path, default=None)
    return p.parse_args(arguments)


def main(arguments: Sequence[str] | None = None) -> int:
    args = _parse(arguments)
    report = run_soak(args)
    out_dir = args.out or (REPOSITORY_ROOT / "evidence" / report["revision"] / "soak")
    report_path, csv_path = _write(report, out_dir)
    print()
    print(f"verdict: {'PASS' if report['verdict']['pass'] else 'FAIL'}")
    for name, c in report["verdict"]["checks"].items():
        flag = "PASS" if c["pass"] else "FAIL"
        print(f"  {flag} {name}: observed={c['observed']} limit={c['limit']}")

    def show(path: Path) -> str:
        try:
            return str(path.relative_to(REPOSITORY_ROOT))
        except ValueError:
            return str(path)

    print(f"report: {show(report_path)}")
    print(f"samples: {show(csv_path)}")
    return 0 if report["verdict"]["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
