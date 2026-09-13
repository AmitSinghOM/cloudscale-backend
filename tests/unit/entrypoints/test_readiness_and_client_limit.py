"""Readiness probe and pre-authentication client rate limiting."""

from __future__ import annotations

import os
import uuid

import pytest
from fastapi.testclient import TestClient

from cloudscale.adapters.projection_readers import StoreProjectionReader
from cloudscale.adapters.sqlite_compat.command_unit_of_work import (
    SqliteCommandUnitOfWork,
)
from cloudscale.adapters.sqlite_compat.dead_letter_store import (
    DeadLetteringProjectionStore,
)
from cloudscale.adapters.sqlite_compat.readiness import SqliteReadinessProbe
from cloudscale.application.command_service import CommandService
from cloudscale.application.query_service import QueryService
from cloudscale.entrypoints.http.app import create_app
from cloudscale.entrypoints.http.settings import HttpSettings

SECRET = "unit-test-secret-at-least-32-bytes-long!!"


class _Probe:
    def __init__(self, fail: Exception | None = None) -> None:
        self.fail = fail
        self.calls = 0

    def check(self) -> dict[str, object]:
        self.calls += 1
        if self.fail:
            raise self.fail
        return {"storage": "ok"}


def _client(tmp_path, probe=None, **overrides) -> tuple[TestClient, list]:
    settings = HttpSettings(jwt_secret=SECRET, **overrides)
    uow = SqliteCommandUnitOfWork(str(tmp_path / "log.db"))
    projection = DeadLetteringProjectionStore(path=str(tmp_path / "p.db"))
    app = create_app(
        settings,
        command_service=CommandService(uow),
        query_service=QueryService(StoreProjectionReader(projection)),
        storage_metadata=projection.metadata,
        readiness_probe=probe,
        closeables=(uow, projection),
    )
    return TestClient(app), [uow, projection]


# -- readiness ----------------------------------------------------------------------


def test_ready_without_probe_is_ready(tmp_path) -> None:
    client, _ = _client(tmp_path)
    with client:
        response = client.get("/v1/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready", "checks": {"probe": "none"}}


def test_ready_calls_probe_and_reports_checks(tmp_path) -> None:
    probe = _Probe()
    client, _ = _client(tmp_path, probe=probe)
    with client:
        response = client.get("/v1/ready")
    assert response.status_code == 200
    assert response.json()["checks"] == {"storage": "ok"}
    assert probe.calls == 1


def test_ready_is_503_and_never_leaks_the_error_message(tmp_path, caplog) -> None:
    """Unauthenticated endpoint: class name only. The message (which can carry
    host names or DSN fragments) must reach the log, not the response."""
    client, _ = _client(
        tmp_path, probe=_Probe(RuntimeError("db unreachable at pg.internal:5432"))
    )
    with client, caplog.at_level("WARNING", logger="cloudscale.http"):
        response = client.get("/v1/ready")
    assert response.status_code == 503
    body = response.json()
    assert body == {"status": "not_ready", "checks": {"error": "RuntimeError"}}
    assert "pg.internal" not in response.text
    assert any("pg.internal" in getattr(r, "error", "") for r in caplog.records)


def test_health_stays_liveness_only_when_probe_fails(tmp_path) -> None:
    client, _ = _client(tmp_path, probe=_Probe(RuntimeError("down")))
    with client:
        assert client.get("/v1/health").status_code == 200
        assert client.get("/v1/ready").status_code == 503


def test_sqlite_probe_reads_each_file(tmp_path) -> None:
    a, b = str(tmp_path / "a.db"), str(tmp_path / "b.db")
    SqliteCommandUnitOfWork(a).close()
    DeadLetteringProjectionStore(path=b).close()
    assert SqliteReadinessProbe(a, b).check() == {"storage": "ok", "databases": 2}


def test_sqlite_probe_fails_on_corrupt_file(tmp_path) -> None:
    bad = tmp_path / "bad.db"
    bad.write_bytes(b"this is not a sqlite database" * 40)
    with pytest.raises(Exception):
        SqliteReadinessProbe(str(bad)).check()


# -- pre-auth client rate limit --------------------------------------------------------


def test_unauthenticated_flood_is_metered_per_client(tmp_path) -> None:
    """Requests with NO token must hit 429 after the client budget — the
    per-subject limiter cannot see them, so this is the only control."""
    client, _ = _client(tmp_path, client_rate_limit_per_minute=5)
    with client:
        statuses = [client.get("/v1/accounts/x/balance").status_code for _ in range(7)]
    assert statuses[:5] == [401] * 5
    assert statuses[5:] == [429] * 2
    with client:
        limited = client.get("/v1/accounts/x/balance")
    assert limited.status_code == 429
    assert int(limited.headers["Retry-After"]) >= 1
    assert limited.json() == {"detail": "rate limit exceeded"}


def test_probes_and_metrics_are_exempt_from_client_limit(tmp_path) -> None:
    client, _ = _client(tmp_path, client_rate_limit_per_minute=1)
    with client:
        assert client.get("/v1/accounts/x/balance").status_code == 401
        assert client.get("/v1/accounts/x/balance").status_code == 429
        for _ in range(3):
            assert client.get("/v1/health").status_code == 200
            assert client.get("/v1/ready").status_code == 200
            assert client.get("/metrics").status_code == 200


def test_forwarded_for_ignored_unless_proxy_trusted(tmp_path) -> None:
    """Spoofing X-Forwarded-For must NOT reset the budget by default."""
    client, _ = _client(tmp_path, client_rate_limit_per_minute=2)
    with client:
        for _ in range(2):
            client.get("/v1/accounts/x/balance")
        spoofed = client.get(
            "/v1/accounts/x/balance",
            headers={"X-Forwarded-For": f"10.0.0.{uuid.uuid4().int % 250}"},
        )
    assert spoofed.status_code == 429


def test_forwarded_for_used_when_proxy_trusted(tmp_path) -> None:
    client, _ = _client(
        tmp_path, client_rate_limit_per_minute=2, trust_proxy_headers=True
    )
    with client:
        for _ in range(2):
            client.get("/v1/accounts/x/balance", headers={"X-Forwarded-For": "1.1.1.1"})
        blocked = client.get(
            "/v1/accounts/x/balance", headers={"X-Forwarded-For": "1.1.1.1, 9.9.9.9"}
        )
        other = client.get(
            "/v1/accounts/x/balance", headers={"X-Forwarded-For": "2.2.2.2"}
        )
    assert blocked.status_code == 429  # left-most entry is the client
    assert other.status_code == 401  # distinct client, own budget


def test_client_limit_disabled_at_zero(tmp_path) -> None:
    client, _ = _client(tmp_path, client_rate_limit_per_minute=0)
    with client:
        statuses = {client.get("/v1/accounts/x/balance").status_code for _ in range(20)}
    assert statuses == {401}


# -- PostgreSQL probe ------------------------------------------------------------------

psycopg = pytest.importorskip("psycopg")
_ADMIN_DSN = os.environ.get("CLOUDSCALE_TEST_PG", "postgresql://localhost/postgres")


def _pg_available() -> bool:
    try:
        with psycopg.connect(_ADMIN_DSN, connect_timeout=2):
            return True
    except psycopg.OperationalError:
        return False


@pytest.mark.skipif(not _pg_available(), reason="no PostgreSQL reachable")
def test_pg_probe_round_trip_and_migration_check(monkeypatch) -> None:
    from cloudscale.adapters.postgres.pool import SchemaNotMigratedError
    from cloudscale.adapters.postgres.readiness import PostgresReadinessProbe

    admin = psycopg.connect(_ADMIN_DSN, autocommit=True)
    db = f"cloudscale_ready_{uuid.uuid4().hex[:8]}"
    admin.execute(f'CREATE DATABASE "{db}"')
    dsn = f"{_ADMIN_DSN.rsplit('/', 1)[0]}/{db}"
    try:
        monkeypatch.setenv("CLOUDSCALE_PG_SCHEMA", "auto")
        probe = PostgresReadinessProbe(dsn)
        checks = probe.check()
        assert checks["storage"] == "ok"
        assert checks["schema_mode"] == "auto"
        assert "schema_revision" not in checks

        # Production mode against an unmigrated database: NOT ready.
        monkeypatch.setenv("CLOUDSCALE_PG_SCHEMA", "migrations")
        with pytest.raises(SchemaNotMigratedError):
            probe.check()
        probe.close()

        # Unreachable database: connection failure surfaces as an exception.
        dead = PostgresReadinessProbe.__new__(PostgresReadinessProbe)
        dead._owns_pool = False
        dead._pool = _ClosedPool()
        with pytest.raises(Exception):
            dead.check()
    finally:
        admin.execute(f'DROP DATABASE "{db}" WITH (FORCE)')
        admin.close()


class _ClosedPool:
    def connection(self):
        raise psycopg.OperationalError("connection refused")
