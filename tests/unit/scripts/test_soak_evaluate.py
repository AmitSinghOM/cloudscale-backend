"""The soak verdict is a pass/fail oracle; its blind spots are what a soak would miss.

``evaluate`` is pure, so these tests build synthetic sample rows and assert the
verdict -- no processes, no PostgreSQL. Locks review-3 findings: a partially
blind PostgreSQL sampler cannot yield PASS, and a run too short to measure
drift cannot yield PASS.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
_SPEC = importlib.util.spec_from_file_location(
    "soak_run", ROOT / "scripts" / "soak_run.py"
)
assert _SPEC is not None and _SPEC.loader is not None
soak = importlib.util.module_from_spec(_SPEC)
sys.modules["soak_run"] = soak
_SPEC.loader.exec_module(soak)


def _healthy_samples(duration: float, interval: float = 30.0) -> list[dict]:
    """Flat memory, low latency, zero errors, retention sawtooth visible."""
    rows = []
    n = int(duration // interval)
    for i in range(n):
        t = i * interval
        rows.append(
            {
                "t": t,
                "server_rss_mb": 120.0,
                "consumer_a_rss_mb": 60.0,
                "consumer_b_rss_mb": 60.0,
                "command_p99_ms": 40.0,
                "query_p99_ms": 10.0,
                "lag_seconds": 0.1,
                "status_5xx": 0,
                "transport_errors": 0,
                "dead_lettered_total": 0,
                "pg_connections": 8,
                # sawtooth: grows, then retention prunes every 10 rows
                "command_results_rows": (i % 10) * 100,
            }
        )
    return rows


def _failed(verdict: dict) -> set[str]:
    return {name for name, c in verdict["checks"].items() if not c["pass"]}


def test_healthy_long_run_passes() -> None:
    duration = soak.MIN_SOAK_SECONDS * 2
    verdict = soak.evaluate(_healthy_samples(duration), None, duration)
    assert verdict["pass"], _failed(verdict)


def test_pg_sampler_errors_fail_the_run_even_when_visible_samples_look_healthy() -> (
    None
):
    duration = soak.MIN_SOAK_SECONDS * 2
    samples = _healthy_samples(duration)
    # Three samples where PostgreSQL refused the sampler: no PG fields at all.
    for row in samples[40:43]:
        for key in ("pg_connections", "command_results_rows"):
            row.pop(key)
        row["pg_error"] = "OperationalError"
    verdict = soak.evaluate(samples, None, duration)
    assert not verdict["pass"]
    assert verdict["checks"]["pg_sampler_errors"]["observed"] == 3
    assert _failed(verdict) == {"pg_sampler_errors"}


def test_run_shorter_than_minimum_cannot_pass() -> None:
    duration = 90.0
    verdict = soak.evaluate(_healthy_samples(duration), None, duration)
    assert not verdict["pass"]
    assert "minimum_duration_s" in _failed(verdict)
    # The RSS criteria are legitimately unmeasurable this early -- they must not
    # be what fails the run, and they must not be what passes it either.
    assert all(
        c["observed"] == "insufficient"
        for name, c in verdict["checks"].items()
        if name.startswith("rss_slope_")
    )
