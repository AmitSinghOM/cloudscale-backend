"""The quickstart's moving parts stay true: dev token → real server → example client.

Spawns the actual uvicorn server and consumer (SQLite tier, temp dir) exactly
as ``make dev`` does, mints a token with ``scripts/dev_token.py``, and runs
``examples/python_client.py`` as a subprocess. If the API changes shape, this
fails before a reader does.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from http_gate_run import _free_port  # noqa: E402

DEV_SECRET = "smoke-test-secret-0123456789abcdef-0123456789abcdef"


def _wait_ready(base_url: str, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"{base_url}/v1/ready", timeout=1.0).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    raise TimeoutError("server never became ready")


@pytest.fixture()
def dev_stack(tmp_path):
    port = _free_port()
    env = dict(
        os.environ,
        CLOUDSCALE_STORAGE="sqlite",
        CLOUDSCALE_LOG_DB=str(tmp_path / "log.db"),
        CLOUDSCALE_PROJECTION_DB=str(tmp_path / "projection.db"),
        CLOUDSCALE_JWT_SECRET=DEV_SECRET,
    )
    procs = [
        subprocess.Popen(
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
            cwd=ROOT,
            env=env,
        ),
        subprocess.Popen(
            (sys.executable, "-m", "cloudscale.entrypoints.consumer_loop"),
            cwd=ROOT,
            env=env,
        ),
    ]
    try:
        _wait_ready(f"http://127.0.0.1:{port}")
        yield f"http://127.0.0.1:{port}", env
    finally:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            proc.wait(timeout=10)


def test_dev_token_and_example_client_run_green_against_a_real_server(
    dev_stack,
) -> None:
    base_url, env = dev_stack
    token = subprocess.run(
        (sys.executable, "scripts/dev_token.py", "--minutes", "5"),
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    ).stdout.strip()
    assert token.count(".") == 2  # a JWT

    result = subprocess.run(
        (sys.executable, "examples/python_client.py", base_url),
        cwd=ROOT,
        env={**env, "CLOUDSCALE_TOKEN": token},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert "balance: 150 at version 2" in result.stdout
    assert "stale write rejected" in result.stdout
    # ADR-0011: the example moves 40 across two streams and proves conservation.
    assert "transfer of 500 rejected: insufficient_funds" in result.stdout
    assert "transfer: accepted → source version 3 / target version 1" in result.stdout
    assert "balances after transfer: 110 + 40 = 150" in result.stdout


def test_dev_token_refuses_to_run_without_a_secret() -> None:
    env = {k: v for k, v in os.environ.items() if k != "CLOUDSCALE_JWT_SECRET"}
    result = subprocess.run(
        (sys.executable, "scripts/dev_token.py"),
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 2
    assert "make dev" in result.stderr
