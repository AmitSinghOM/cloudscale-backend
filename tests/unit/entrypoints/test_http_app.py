"""HTTP tier contracts: auth is fail-closed, health discloses storage tier,
the query endpoint speaks through the typed application layer."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import jwt
import pytest
from fastapi.testclient import TestClient

from cloudscale.adapters.projection_readers import StoreProjectionReader
from cloudscale.adapters.sqlite_compat.dead_letter_store import (
    DeadLetteringProjectionStore,
)
from cloudscale.application.query_service import QueryService
from cloudscale.entrypoints.http.app import create_app
from cloudscale.entrypoints.http.settings import HttpSettings

SECRET = "unit-test-secret-0123456789abcdef-0123456789abcdef"


def _settings(**overrides: object) -> HttpSettings:
    values: dict = {"jwt_secret": SECRET}
    values.update(overrides)
    return HttpSettings(**values)


def _token(
    *,
    secret: str = SECRET,
    issuer: str = "cloudscale",
    subject: str | None = "user-1",
    expires_in: int = 300,
) -> str:
    claims: dict = {
        "iss": issuer,
        "exp": datetime.now(UTC) + timedelta(seconds=expires_in),
    }
    if subject is not None:
        claims["sub"] = subject
    return jwt.encode(claims, secret, algorithm="HS256")


@pytest.fixture()
def client() -> TestClient:
    projection = DeadLetteringProjectionStore(path=":memory:")
    projection.apply(
        {
            "event_id": "evt-1",
            "id": 1,
            "type": "Deposited",
            "account_id": "acct-1",
            "amount": 75,
        }
    )
    app = create_app(
        _settings(),
        query_service=QueryService(StoreProjectionReader(projection)),
        storage_metadata=projection.metadata,
    )
    return TestClient(app)


def test_health_is_public_and_discloses_storage_tier(client: TestClient) -> None:
    response = client.get("/v1/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["storage"]["storage_tier"] == "sqlite-compatibility"
    assert body["storage"]["production"] is False


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": "Bearer not-a-jwt"},
        {"Authorization": f"Basic {_token()}"},
        {
            "Authorization": "Bearer "
            + _token(secret="wrong-secret-0123456789abcdef-0123456789abcdef")
        },
        {"Authorization": f"Bearer {_token(issuer='someone-else')}"},
        {"Authorization": f"Bearer {_token(subject=None)}"},
        {"Authorization": f"Bearer {_token(expires_in=-60)}"},
    ],
)
def test_query_rejects_every_invalid_credential(
    client: TestClient, headers: dict
) -> None:
    response = client.get("/v1/accounts/acct-1/balance", headers=headers)
    assert response.status_code == 401
    assert response.headers.get("WWW-Authenticate") == "Bearer"


def test_query_returns_the_projected_balance(client: TestClient) -> None:
    response = client.get(
        "/v1/accounts/acct-1/balance",
        headers={"Authorization": f"Bearer {_token()}"},
    )
    assert response.status_code == 200
    assert response.json() == {
        "account_id": "acct-1",
        "balance": 75,
        "version": 1,
        "consistency": "eventual",
    }


def test_unknown_account_is_404(client: TestClient) -> None:
    response = client.get(
        "/v1/accounts/no-such-acct/balance",
        headers={"Authorization": f"Bearer {_token()}"},
    )
    assert response.status_code == 404


def test_query_auth_can_be_relaxed_explicitly() -> None:
    projection = DeadLetteringProjectionStore(path=":memory:")
    app = create_app(
        _settings(query_auth_required=False),
        query_service=QueryService(StoreProjectionReader(projection)),
        storage_metadata=projection.metadata,
    )
    response = TestClient(app).get("/v1/accounts/acct-1/balance")
    assert response.status_code == 404  # authorized through, account absent


def test_settings_require_a_real_secret() -> None:
    with pytest.raises(Exception):
        HttpSettings(jwt_secret="short")
