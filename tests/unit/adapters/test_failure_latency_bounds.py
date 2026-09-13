"""Failure-latency bounds: pool exhaustion and JWKS fetch must fail fast."""

from __future__ import annotations

import os
import threading
import time
import uuid
from unittest import mock

import pytest

from cloudscale.entrypoints.http.verifiers import (
    JWKS_FETCH_TIMEOUT_SECONDS,
    JwksVerifier,
)


def test_jwks_client_is_built_with_a_tight_fetch_timeout() -> None:
    with mock.patch("cloudscale.entrypoints.http.verifiers.PyJWKClient") as factory:
        JwksVerifier("https://issuer.example/jwks", algorithms=["RS256"], issuer="i")
    kwargs = factory.call_args.kwargs
    assert kwargs["timeout"] == JWKS_FETCH_TIMEOUT_SECONDS
    assert JWKS_FETCH_TIMEOUT_SECONDS <= 5  # never the library's 30 s default


# -- PostgreSQL pool -----------------------------------------------------------------

psycopg = pytest.importorskip("psycopg")
_ADMIN_DSN = os.environ.get("CLOUDSCALE_TEST_PG", "postgresql://localhost/postgres")


def _pg_available() -> bool:
    try:
        with psycopg.connect(_ADMIN_DSN, connect_timeout=2):
            return True
    except psycopg.OperationalError:
        return False


@pytest.mark.skipif(not _pg_available(), reason="no PostgreSQL reachable")
def test_pool_exhaustion_fails_fast_as_operational_error(monkeypatch) -> None:
    """Every connection busy -> the next acquire raises within the bound, and
    the exception is an OperationalError so the command breaker counts it."""
    from cloudscale.adapters.postgres.pool import open_pool

    monkeypatch.setenv("CLOUDSCALE_PG_POOL_TIMEOUT_SECONDS", "0.5")
    pool = open_pool(_ADMIN_DSN, max_size=1)
    release = threading.Event()

    def hold() -> None:
        with pool.connection():
            release.wait(timeout=10)

    holder = threading.Thread(target=hold, daemon=True)
    holder.start()
    time.sleep(0.2)  # let the holder take the only connection
    try:
        started = time.perf_counter()
        with pytest.raises(psycopg.OperationalError):
            with pool.connection():
                pass
        elapsed = time.perf_counter() - started
        assert 0.4 <= elapsed < 3.0, elapsed  # bounded by the setting, not 30 s
    finally:
        release.set()
        holder.join(timeout=5)
        pool.close()


def test_pool_timeout_setting_must_be_positive(monkeypatch) -> None:
    from cloudscale.adapters.postgres.pool import open_pool

    monkeypatch.setenv("CLOUDSCALE_PG_POOL_TIMEOUT_SECONDS", "0")
    with pytest.raises(ValueError, match="acquisition timeout"):
        open_pool(f"postgresql://localhost/nonexistent_{uuid.uuid4().hex[:6]}")
