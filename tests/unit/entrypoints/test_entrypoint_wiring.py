"""Env-driven wiring contracts for the real-process entrypoints."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from cloudscale.entrypoints.consumer_loop import build_storage
from cloudscale.entrypoints.http.main import build_app

SECRET = "entrypoint-test-secret-0123456789abcdef-0123456789"


def _base_env(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("CLOUDSCALE_JWT_SECRET", SECRET)
    monkeypatch.setenv("CLOUDSCALE_LOG_DB", str(tmp_path / "log.db"))
    monkeypatch.setenv("CLOUDSCALE_PROJECTION_DB", str(tmp_path / "projection.db"))


def test_build_app_sqlite_defaults_and_serves_health(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    _base_env(monkeypatch, tmp_path)
    monkeypatch.delenv("CLOUDSCALE_STORAGE", raising=False)

    app = build_app()
    response = TestClient(app).get("/v1/health")
    assert response.status_code == 200
    assert response.json()["storage"]["storage_tier"] == "sqlite-compatibility"


def test_build_app_rejects_unknown_storage(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    _base_env(monkeypatch, tmp_path)
    monkeypatch.setenv("CLOUDSCALE_STORAGE", "kafka")
    with pytest.raises(ValueError, match="unsupported CLOUDSCALE_STORAGE"):
        build_app()


def test_build_app_fails_closed_without_a_jwt_secret(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    _base_env(monkeypatch, tmp_path)
    monkeypatch.delenv("CLOUDSCALE_JWT_SECRET")
    with pytest.raises(Exception):
        build_app()


def test_consumer_storage_selection_sqlite(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    import sqlite3

    _base_env(monkeypatch, tmp_path)
    feed, projection, retryable, lease = build_storage("sqlite")
    try:
        assert retryable == (sqlite3.OperationalError,)
        assert lease.try_acquire() is True  # SQLite: NoLease
        assert feed.read_all(0) == []
        assert projection.last_id() == 0
    finally:
        projection.close()
        feed.close()


def test_consumer_storage_selection_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="unsupported CLOUDSCALE_STORAGE"):
        build_storage("kafka")
