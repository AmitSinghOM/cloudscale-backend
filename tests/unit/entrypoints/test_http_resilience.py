"""Resilience and OTel wiring on the HTTP command path.

Transient storage failures are retried behind the endpoint; an exhausted
budget or an open circuit maps to 503 + Retry-After (safe to retry with the
same command_id); deterministic rejections never trip the breaker; OTel
instrumentation emits server spans when a provider is supplied.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime, timedelta

import jwt
from fastapi.testclient import TestClient

from cloudscale.adapters.projection_readers import StoreProjectionReader
from cloudscale.adapters.sqlite_compat.command_unit_of_work import (
    SqliteCommandUnitOfWork,
)
from cloudscale.adapters.sqlite_compat.dead_letter_store import (
    DeadLetteringProjectionStore,
)
from cloudscale.adapters.telemetry import configure_in_memory_provider
from cloudscale.application.command_service import CommandService
from cloudscale.application.query_service import QueryService
from cloudscale.entrypoints.http.app import create_app
from cloudscale.entrypoints.http.settings import HttpSettings
from cloudscale.resilience import CircuitBreaker, RetryPolicy

SECRET = "resilience-test-secret-0123456789abcdef-0123456789"


def _token() -> dict:
    token = jwt.encode(
        {
            "iss": "cloudscale",
            "sub": "user-1",
            "scope": "accounts:admin",
            "exp": datetime.now(UTC) + timedelta(minutes=5),
        },
        SECRET,
        algorithm="HS256",
    )
    return {"Authorization": f"Bearer {token}"}


def _body(**overrides: object) -> dict:
    body: dict = {
        "command_id": str(uuid.uuid4()),
        "type": "deposit",
        "amount": 100,
        "expected_version": 0,
    }
    body.update(overrides)
    return body


class _FlakyUnitOfWork:
    """Fails ``failures`` executes with OperationalError, then delegates."""

    def __init__(self, inner: SqliteCommandUnitOfWork, failures: int) -> None:
        self._inner = inner
        self.failures = failures
        self.calls = 0

    def execute(self, request):
        self.calls += 1
        if self.calls <= self.failures:
            raise sqlite3.OperationalError("database is locked")
        return self._inner.execute(request)


def _build(tmp_path, uow, **app_overrides):
    projection = DeadLetteringProjectionStore(path=str(tmp_path / "projection.db"))
    app = create_app(
        HttpSettings(jwt_secret=SECRET),
        command_service=CommandService(uow),
        query_service=QueryService(StoreProjectionReader(projection)),
        storage_metadata=projection.metadata,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0.0),
        **app_overrides,
    )
    return TestClient(app)


def test_transient_failures_are_retried_to_success(tmp_path) -> None:
    inner = SqliteCommandUnitOfWork(str(tmp_path / "log.db"))
    flaky = _FlakyUnitOfWork(inner, failures=2)
    client = _build(tmp_path, flaky)

    response = client.post(
        "/v1/accounts/acct-1/commands", json=_body(), headers=_token()
    )
    assert response.status_code == 201
    assert flaky.calls == 3


def test_exhausted_retries_map_to_503_with_retry_after(tmp_path) -> None:
    inner = SqliteCommandUnitOfWork(str(tmp_path / "log.db"))
    flaky = _FlakyUnitOfWork(inner, failures=99)
    client = _build(
        tmp_path,
        flaky,
        breaker=CircuitBreaker(
            failure_threshold=100, counted_errors=(sqlite3.OperationalError,)
        ),
    )

    response = client.post(
        "/v1/accounts/acct-1/commands", json=_body(), headers=_token()
    )
    assert response.status_code == 503
    assert response.headers.get("Retry-After") == "1"
    assert flaky.calls == 3  # full budget spent


def test_open_circuit_fails_fast_with_retry_after(tmp_path) -> None:
    inner = SqliteCommandUnitOfWork(str(tmp_path / "log.db"))
    flaky = _FlakyUnitOfWork(inner, failures=99)
    client = _build(
        tmp_path,
        flaky,
        breaker=CircuitBreaker(
            failure_threshold=2,
            reset_timeout_seconds=3600.0,
            counted_errors=(sqlite3.OperationalError,),
        ),
    )

    first = client.post("/v1/accounts/acct-1/commands", json=_body(), headers=_token())
    assert first.status_code == 503

    calls_before = flaky.calls
    second = client.post("/v1/accounts/acct-1/commands", json=_body(), headers=_token())
    assert second.status_code == 503
    assert int(second.headers["Retry-After"]) >= 1
    assert flaky.calls == calls_before  # circuit open: unit of work not touched


def test_deterministic_rejections_do_not_trip_the_breaker(tmp_path) -> None:
    inner = SqliteCommandUnitOfWork(str(tmp_path / "log.db"))
    client = _build(
        tmp_path,
        inner,
        breaker=CircuitBreaker(
            failure_threshold=1, counted_errors=(sqlite3.OperationalError,)
        ),
    )

    # Repeated 422s (insufficient funds) must not open a threshold-1 breaker.
    for _ in range(3):
        response = client.post(
            "/v1/accounts/acct-1/commands",
            json=_body(type="withdraw", amount=50),
            headers=_token(),
        )
        assert response.status_code == 422

    accepted = client.post(
        "/v1/accounts/acct-1/commands", json=_body(), headers=_token()
    )
    assert accepted.status_code == 201


def test_otel_provider_emits_server_spans(tmp_path) -> None:
    provider, exporter = configure_in_memory_provider("http-test")
    inner = SqliteCommandUnitOfWork(str(tmp_path / "log.db"))
    client = _build(tmp_path, inner, tracer_provider=provider)

    assert (
        client.post(
            "/v1/accounts/acct-1/commands", json=_body(), headers=_token()
        ).status_code
        == 201
    )
    span_names = [span.name for span in exporter.get_finished_spans()]
    assert any("/commands" in name for name in span_names)
