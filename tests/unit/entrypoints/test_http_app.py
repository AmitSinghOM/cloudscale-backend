"""HTTP tier contracts: auth is fail-closed, health discloses storage tier,
the query endpoint speaks through the typed application layer."""

from __future__ import annotations

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
from cloudscale.entrypoints.http.settings import HttpSettings
from cloudscale.processes.resilient_consumer import ResilientConsumer
from cqrs import SqliteEventStore

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


class _Stack:
    """Full local stack over one shared SQLite log file."""

    def __init__(self, tmp_path, query_auth_required: bool = True) -> None:
        self.log_path = str(tmp_path / "log.db")
        self.uow = SqliteCommandUnitOfWork(self.log_path)
        self.projection = DeadLetteringProjectionStore(
            path=str(tmp_path / "projection.db")
        )
        app = create_app(
            _settings(query_auth_required=query_auth_required),
            command_service=CommandService(self.uow),
            query_service=QueryService(StoreProjectionReader(self.projection)),
            storage_metadata=self.projection.metadata,
        )
        self.client = TestClient(app)

    def run_consumer(self) -> None:
        feed = SqliteEventStore(self.log_path)
        try:
            ResilientConsumer(feed, self.projection).run()
        finally:
            feed.close()


@pytest.fixture()
def stack(tmp_path) -> _Stack:
    built = _Stack(tmp_path)
    yield built
    built.uow.close()
    built.projection.close()


@pytest.fixture()
def client(stack: _Stack) -> TestClient:
    stack.projection.apply(
        {
            "event_id": "evt-1",
            "id": 1,
            "type": "Deposited",
            "account_id": "acct-1",
            "amount": 75,
        }
    )
    return stack.client


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


def test_query_auth_can_be_relaxed_explicitly(tmp_path) -> None:
    relaxed_dir = tmp_path / "relaxed"
    relaxed_dir.mkdir()
    stack = _Stack(relaxed_dir, query_auth_required=False)
    response = stack.client.get("/v1/accounts/acct-1/balance")
    assert response.status_code == 404  # authorized through, account absent


def test_settings_require_a_real_secret() -> None:
    with pytest.raises(Exception):
        HttpSettings(jwt_secret="short")


# -- command endpoint ----------------------------------------------------------


def _command_body(**overrides: object) -> dict:
    body: dict = {
        "command_id": str(uuid.uuid4()),
        "type": "deposit",
        "amount": 100,
        "expected_version": 0,
    }
    body.update(overrides)
    return body


def _auth() -> dict:
    return {"Authorization": f"Bearer {_token()}"}


def test_commands_always_require_auth_even_when_queries_are_relaxed(
    tmp_path,
) -> None:
    cmd_dir = tmp_path / "cmd-auth"
    cmd_dir.mkdir()
    stack = _Stack(cmd_dir, query_auth_required=False)
    response = stack.client.post("/v1/accounts/acct-9/commands", json=_command_body())
    assert response.status_code == 401


def test_accepted_command_returns_201_with_result_fields(stack: _Stack) -> None:
    response = stack.client.post(
        "/v1/accounts/acct-9/commands", json=_command_body(), headers=_auth()
    )
    assert response.status_code == 201
    body = response.json()
    assert body["outcome"] == "accepted"
    assert body["committed_version"] == 1
    assert body["event_id"] is not None
    assert body["error_code"] is None


def test_command_replay_and_conflict_semantics_over_http(stack: _Stack) -> None:
    body = _command_body()
    first = stack.client.post(
        "/v1/accounts/acct-9/commands", json=body, headers=_auth()
    )
    replay = stack.client.post(
        "/v1/accounts/acct-9/commands", json=body, headers=_auth()
    )
    assert replay.status_code == 201
    assert replay.json() == first.json()  # the original persisted result

    conflicting = dict(body, amount=999)
    conflict = stack.client.post(
        "/v1/accounts/acct-9/commands", json=conflicting, headers=_auth()
    )
    assert conflict.status_code == 409
    assert conflict.json()["outcome"] == "command_id_conflict"


def test_rejections_map_to_domain_statuses(stack: _Stack) -> None:
    stale = stack.client.post(
        "/v1/accounts/acct-9/commands",
        json=_command_body(expected_version=5),
        headers=_auth(),
    )
    assert stale.status_code == 409
    assert stale.json()["outcome"] == "version_conflict"

    overdraft = stack.client.post(
        "/v1/accounts/acct-9/commands",
        json=_command_body(type="withdraw", amount=50),
        headers=_auth(),
    )
    assert overdraft.status_code == 422
    assert overdraft.json()["outcome"] == "insufficient_funds"

    invalid = stack.client.post(
        "/v1/accounts/acct-9/commands",
        json=_command_body(amount=-5),
        headers=_auth(),
    )
    assert invalid.status_code == 400


def test_end_to_end_command_to_consumer_to_query(stack: _Stack) -> None:
    """The full pipeline: POST -> unit of work -> log -> consumer -> GET."""
    deposit = stack.client.post(
        "/v1/accounts/acct-e2e/commands",
        json=_command_body(amount=500),
        headers=_auth(),
    )
    assert deposit.status_code == 201
    withdraw = stack.client.post(
        "/v1/accounts/acct-e2e/commands",
        json=_command_body(type="withdraw", amount=120, expected_version=1),
        headers=_auth(),
    )
    assert withdraw.status_code == 201

    # Before the consumer runs, the read model trails the log.
    assert (
        stack.client.get("/v1/accounts/acct-e2e/balance", headers=_auth()).status_code
        == 404
    )

    stack.run_consumer()

    response = stack.client.get("/v1/accounts/acct-e2e/balance", headers=_auth())
    assert response.status_code == 200
    assert response.json() == {
        "account_id": "acct-e2e",
        "balance": 380,
        "version": 2,
        "consistency": "eventual",
    }
