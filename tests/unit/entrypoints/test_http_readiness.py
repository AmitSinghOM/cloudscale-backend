"""Production-readiness controls on the HTTP tier.

Authorization is claims-based and default-deny; abuse controls are on by
default; every command decision is audit-logged and counted; shutdown closes
injected resources; 401s never fingerprint the token parser.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta

import jwt
import pytest
from fastapi.testclient import TestClient

from cloudscale.adapters.projection_readers import StoreProjectionReader
from cloudscale.adapters.sqlite_compat.command_unit_of_work import (
    SqliteCommandUnitOfWork,
)
from cloudscale.adapters.sqlite_compat.dead_letter_store import (
    DeadLetteringProjectionStore,
)
from cloudscale.application.command_service import CommandService
from cloudscale.application.query_service import QueryService
from cloudscale.entrypoints.http.app import create_app
from cloudscale.entrypoints.http.limits import RateLimiter
from cloudscale.entrypoints.http.settings import HttpSettings

SECRET = "readiness-test-secret-0123456789abcdef-0123456789abcd"


def _token(**claims: object) -> str:
    payload: dict = {
        "iss": "cloudscale",
        "sub": "alice",
        "iat": datetime.now(UTC),
        "exp": datetime.now(UTC) + timedelta(minutes=5),
    }
    payload.update(claims)
    if payload.get("scope") == "accounts:admin" and "jti" not in payload:
        payload["jti"] = "test-jti-" + uuid.uuid4().hex
    return jwt.encode(payload, SECRET, algorithm="HS256")


def _auth(**claims: object) -> dict:
    return {"Authorization": f"Bearer {_token(**claims)}"}


def _body(**overrides: object) -> dict:
    body: dict = {
        "command_id": str(uuid.uuid4()),
        "type": "deposit",
        "amount": 100,
        "expected_version": 0,
    }
    body.update(overrides)
    return body


class _Closeable:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _build(tmp_path, *, settings: HttpSettings | None = None, **overrides):
    uow = SqliteCommandUnitOfWork(str(tmp_path / "log.db"))
    projection = DeadLetteringProjectionStore(path=str(tmp_path / "projection.db"))
    app = create_app(
        settings or HttpSettings(jwt_secret=SECRET),
        command_service=CommandService(uow),
        query_service=QueryService(StoreProjectionReader(projection)),
        storage_metadata=projection.metadata,
        **overrides,
    )
    return app


# -- authorization ---------------------------------------------------------------


def test_token_without_account_grants_is_forbidden_everywhere(tmp_path) -> None:
    client = TestClient(_build(tmp_path))
    assert (
        client.post(
            "/v1/accounts/acct-1/commands", json=_body(), headers=_auth()
        ).status_code
        == 403
    )
    assert client.get("/v1/accounts/acct-1/balance", headers=_auth()).status_code == 403


def test_accounts_claim_scopes_access_to_named_accounts_only(tmp_path) -> None:
    client = TestClient(_build(tmp_path))
    mine = _auth(accounts=["acct-mine"])
    assert (
        client.post(
            "/v1/accounts/acct-mine/commands", json=_body(), headers=mine
        ).status_code
        == 201
    )
    assert (
        client.post(
            "/v1/accounts/acct-theirs/commands", json=_body(), headers=mine
        ).status_code
        == 403
    )
    assert (
        client.get("/v1/accounts/acct-theirs/balance", headers=mine).status_code == 403
    )


def test_admin_scope_grants_all_accounts(tmp_path) -> None:
    client = TestClient(_build(tmp_path))
    admin = _auth(scope="accounts:admin")
    for account in ("a", "b"):
        assert (
            client.post(
                f"/v1/accounts/{account}/commands", json=_body(), headers=admin
            ).status_code
            == 201
        )


def test_malformed_accounts_claim_is_rejected(tmp_path) -> None:
    client = TestClient(_build(tmp_path))
    assert (
        client.get(
            "/v1/accounts/x/balance", headers=_auth(accounts="not-a-list")
        ).status_code
        == 401
    )


# -- 401 hardening / audience ------------------------------------------------------


def test_401_detail_is_fixed_and_does_not_name_the_parser_error(tmp_path) -> None:
    client = TestClient(_build(tmp_path))
    bad = jwt.encode({"iss": "cloudscale", "sub": "x"}, "wrong-" * 8, algorithm="HS256")
    response = client.get(
        "/v1/accounts/x/balance", headers={"Authorization": f"Bearer {bad}"}
    )
    assert response.status_code == 401
    assert response.json()["detail"] == "invalid or expired bearer token"
    assert "Signature" not in response.text and "Error" not in response.text


def test_audience_is_enforced_when_configured(tmp_path) -> None:
    settings = HttpSettings(jwt_secret=SECRET, jwt_audience="cloudscale-api")
    client = TestClient(_build(tmp_path, settings=settings))
    no_aud = _auth(scope="accounts:admin")
    assert client.get("/v1/accounts/x/balance", headers=no_aud).status_code == 401
    right_aud = _auth(scope="accounts:admin", aud="cloudscale-api")
    assert client.get("/v1/accounts/x/balance", headers=right_aud).status_code == 404


# -- abuse controls -------------------------------------------------------------------


def test_rate_limit_is_per_subject_and_returns_429_with_retry_after(tmp_path) -> None:
    clock = {"now": 0.0}
    limiter = RateLimiter(per_minute=3, clock=lambda: clock["now"])
    client = TestClient(_build(tmp_path, rate_limiter=limiter))
    alice = _auth(scope="accounts:admin")
    bob = _auth(scope="accounts:admin", sub="bob")

    for _ in range(3):
        assert client.get("/v1/accounts/x/balance", headers=alice).status_code == 404
    limited = client.get("/v1/accounts/x/balance", headers=alice)
    assert limited.status_code == 429
    assert int(limited.headers["Retry-After"]) >= 1
    # Another subject has its own bucket.
    assert client.get("/v1/accounts/x/balance", headers=bob).status_code == 404
    # Refill restores service.
    clock["now"] += 60.0
    assert client.get("/v1/accounts/x/balance", headers=alice).status_code == 404


def test_rate_limit_can_be_disabled_explicitly(tmp_path) -> None:
    settings = HttpSettings(jwt_secret=SECRET, rate_limit_per_minute=0)
    client = TestClient(_build(tmp_path, settings=settings))
    admin = _auth(scope="accounts:admin")
    for _ in range(50):
        assert client.get("/v1/accounts/x/balance", headers=admin).status_code == 404


def test_oversized_body_is_rejected_with_413(tmp_path) -> None:
    settings = HttpSettings(jwt_secret=SECRET, max_body_bytes=200)
    client = TestClient(_build(tmp_path, settings=settings))
    big = _body(command_id="x" * 400)
    response = client.post(
        "/v1/accounts/a/commands", json=big, headers=_auth(scope="accounts:admin")
    )
    assert response.status_code == 413


def test_unknown_fields_are_rejected_not_ignored(tmp_path) -> None:
    client = TestClient(_build(tmp_path))
    response = client.post(
        "/v1/accounts/a/commands",
        json=_body(expected_verison=0),  # typo must not silently pass
        headers=_auth(scope="accounts:admin"),
    )
    assert response.status_code == 422


def test_cors_is_closed_by_default_and_opens_only_to_allowlist(tmp_path) -> None:
    closed = TestClient(_build(tmp_path))
    preflight = closed.options(
        "/v1/accounts/a/balance",
        headers={
            "Origin": "https://evil.example",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert "access-control-allow-origin" not in preflight.headers

    settings = HttpSettings(jwt_secret=SECRET, cors_origins=["https://app.example"])
    cors_dir = tmp_path / "cors"
    cors_dir.mkdir()
    opened = TestClient(_build(cors_dir, settings=settings))
    preflight = opened.options(
        "/v1/accounts/a/balance",
        headers={
            "Origin": "https://app.example",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert preflight.headers.get("access-control-allow-origin") == "https://app.example"


# -- observability ------------------------------------------------------------------------


def test_every_command_decision_is_audit_logged(
    tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    client = TestClient(_build(tmp_path))
    admin = _auth(scope="accounts:admin")
    body = _body(amount=40)
    with caplog.at_level(logging.INFO, logger="cloudscale.audit"):
        client.post("/v1/accounts/acct-a/commands", json=body, headers=admin)
        client.post(
            "/v1/accounts/acct-a/commands",
            json=_body(type="withdraw", amount=999, expected_version=1),
            headers=admin,
        )
    records = [r for r in caplog.records if r.name == "cloudscale.audit"]
    assert [r.outcome for r in records] == ["accepted", "insufficient_funds"]  # type: ignore[attr-defined]
    assert records[0].subject == "alice"  # type: ignore[attr-defined]
    assert records[0].account_id == "acct-a"  # type: ignore[attr-defined]
    assert records[0].command_id == body["command_id"]  # type: ignore[attr-defined]
    # The bearer token must never reach the audit log.
    assert "Bearer" not in caplog.text


def test_metrics_endpoint_counts_requests_and_command_outcomes(tmp_path) -> None:
    client = TestClient(_build(tmp_path))
    admin = _auth(scope="accounts:admin")
    client.post("/v1/accounts/acct-a/commands", json=_body(), headers=admin)
    client.get("/v1/accounts/acct-a/balance", headers=admin)

    metrics = client.get("/metrics")
    assert metrics.status_code == 200
    text = metrics.text
    assert 'cloudscale_command_outcomes_total{outcome="accepted"} 1.0' in text
    assert 'route="/v1/accounts/{account_id}/commands",status="201"' in text
    assert "cloudscale_http_request_seconds_bucket" in text


# -- lifecycle -------------------------------------------------------------------------------


def test_shutdown_closes_injected_resources(tmp_path) -> None:
    resource = _Closeable()
    app = _build(tmp_path, closeables=(resource,))
    with TestClient(app):
        assert resource.closed is False
    assert resource.closed is True
