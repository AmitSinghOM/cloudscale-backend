"""Account ownership registry: adapter semantics and HTTP ownership flows.

The system now has its own record of who owns an account. Registration is
idempotent for the owner, refuses reassignment, and grants the owner (and
only the owner, absent claims/admin) access to command and query it.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta

import jwt
import pytest
from fastapi.testclient import TestClient

from cloudscale.adapters.projection_readers import StoreProjectionReader
from cloudscale.adapters.sqlite_compat.account_registry import SqliteAccountRegistry
from cloudscale.adapters.sqlite_compat.command_unit_of_work import (
    SqliteCommandUnitOfWork,
)
from cloudscale.adapters.sqlite_compat.dead_letter_store import (
    DeadLetteringProjectionStore,
)
from cloudscale.application.command_service import CommandService
from cloudscale.application.ports import RegistrationOutcome
from cloudscale.application.query_service import QueryService
from cloudscale.entrypoints.http.app import create_app
from cloudscale.entrypoints.http.settings import HttpSettings

SECRET = "registry-test-secret-0123456789abcdef-0123456789abcdef"


# -- adapter -----------------------------------------------------------------------


def test_register_is_idempotent_for_owner_and_refuses_reassignment(tmp_path) -> None:
    registry = SqliteAccountRegistry(str(tmp_path / "r.db"))
    try:
        assert registry.owner_of("acct-1") is None
        assert registry.register("acct-1", "alice") is RegistrationOutcome.CREATED
        assert (
            registry.register("acct-1", "alice")
            is RegistrationOutcome.ALREADY_OWNED_BY_CALLER
        )
        assert registry.register("acct-1", "bob") is RegistrationOutcome.TAKEN
        assert registry.owner_of("acct-1") == "alice"
    finally:
        registry.close()


def test_register_validates_inputs(tmp_path) -> None:
    registry = SqliteAccountRegistry(str(tmp_path / "r.db"))
    try:
        with pytest.raises(Exception):
            registry.register("", "alice")
        with pytest.raises(ValueError):
            registry.register("acct-1", "")
    finally:
        registry.close()


def test_registry_survives_reopen(tmp_path) -> None:
    path = str(tmp_path / "r.db")
    first = SqliteAccountRegistry(path)
    first.register("acct-1", "alice")
    first.close()
    second = SqliteAccountRegistry(path)
    try:
        assert second.owner_of("acct-1") == "alice"
    finally:
        second.close()


# -- HTTP flows ---------------------------------------------------------------------


def _auth(subject: str, **claims: object) -> dict:
    payload: dict = {
        "iss": "cloudscale",
        "sub": subject,
        "exp": datetime.now(UTC) + timedelta(minutes=5),
    }
    payload.update(claims)
    return {"Authorization": f"Bearer {jwt.encode(payload, SECRET, algorithm='HS256')}"}


def _body(**overrides: object) -> dict:
    body: dict = {
        "command_id": str(uuid.uuid4()),
        "type": "deposit",
        "amount": 100,
        "expected_version": 0,
    }
    body.update(overrides)
    return body


@pytest.fixture()
def client(tmp_path) -> TestClient:
    uow = SqliteCommandUnitOfWork(str(tmp_path / "log.db"))
    projection = DeadLetteringProjectionStore(path=str(tmp_path / "projection.db"))
    registry = SqliteAccountRegistry(str(tmp_path / "log.db"))
    app = create_app(
        HttpSettings(jwt_secret=SECRET, rate_limit_per_minute=0),
        command_service=CommandService(uow),
        query_service=QueryService(StoreProjectionReader(projection)),
        storage_metadata=projection.metadata,
        account_registry=registry,
        closeables=(uow, projection, registry),
    )
    with TestClient(app) as test_client:
        yield test_client


def test_registration_grants_owner_access_and_denies_others(client: TestClient) -> None:
    alice, bob = _auth("alice"), _auth("bob")

    # Unregistered, unnamed account: denied for everyone without admin.
    assert (
        client.post(
            "/v1/accounts/acct-a/commands", json=_body(), headers=alice
        ).status_code
        == 403
    )

    created = client.post("/v1/accounts", json={"account_id": "acct-a"}, headers=alice)
    assert created.status_code == 201
    assert created.json() == {
        "account_id": "acct-a",
        "outcome": "created",
        "owner": "alice",
    }

    # Owner may command and (after projection) query; bob may do neither.
    assert (
        client.post(
            "/v1/accounts/acct-a/commands", json=_body(), headers=alice
        ).status_code
        == 201
    )
    assert (
        client.post(
            "/v1/accounts/acct-a/commands", json=_body(expected_version=1), headers=bob
        ).status_code
        == 403
    )
    assert client.get("/v1/accounts/acct-a/balance", headers=bob).status_code == 403
    # Owner's query is authorized (404 only because the consumer hasn't run).
    assert client.get("/v1/accounts/acct-a/balance", headers=alice).status_code == 404


def test_registration_is_idempotent_and_refuses_takeover(client: TestClient) -> None:
    alice, bob = _auth("alice"), _auth("bob")
    assert (
        client.post(
            "/v1/accounts", json={"account_id": "acct-b"}, headers=alice
        ).status_code
        == 201
    )
    again = client.post("/v1/accounts", json={"account_id": "acct-b"}, headers=alice)
    assert again.status_code == 200
    assert again.json()["outcome"] == "already_owned_by_caller"

    taken = client.post("/v1/accounts", json={"account_id": "acct-b"}, headers=bob)
    assert taken.status_code == 409
    assert taken.json() == {"account_id": "acct-b", "outcome": "taken", "owner": None}


def test_claims_and_admin_still_grant_access_alongside_registry(
    client: TestClient,
) -> None:
    client.post("/v1/accounts", json={"account_id": "acct-c"}, headers=_auth("alice"))
    # A different subject explicitly named on the account by the issuer.
    named = _auth("service", accounts=["acct-c"])
    assert (
        client.post(
            "/v1/accounts/acct-c/commands", json=_body(), headers=named
        ).status_code
        == 201
    )
    admin = _auth("ops", scope="accounts:admin")
    assert client.get("/v1/accounts/acct-c/balance", headers=admin).status_code == 404


def test_registration_requires_auth_and_rejects_bad_input(client: TestClient) -> None:
    assert client.post("/v1/accounts", json={"account_id": "x"}).status_code == 401
    alice = _auth("alice")
    assert (
        client.post("/v1/accounts", json={"account_id": ""}, headers=alice).status_code
        == 400
    )
    assert (
        client.post(
            "/v1/accounts", json={"account_id": "x", "extra": 1}, headers=alice
        ).status_code
        == 422
    )


def test_registrations_are_audit_logged(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.INFO, logger="cloudscale.audit"):
        client.post(
            "/v1/accounts", json={"account_id": "acct-d"}, headers=_auth("alice")
        )
        client.post("/v1/accounts", json={"account_id": "acct-d"}, headers=_auth("bob"))
    records = [r for r in caplog.records if r.getMessage() == "account.registered"]
    assert [(r.subject, r.outcome) for r in records] == [  # type: ignore[attr-defined]
        ("alice", "created"),
        ("bob", "taken"),
    ]


def test_registration_endpoint_is_501_when_no_registry_is_wired(tmp_path) -> None:
    uow = SqliteCommandUnitOfWork(str(tmp_path / "log.db"))
    projection = DeadLetteringProjectionStore(path=str(tmp_path / "p.db"))
    app = create_app(
        HttpSettings(jwt_secret=SECRET),
        command_service=CommandService(uow),
        query_service=QueryService(StoreProjectionReader(projection)),
        storage_metadata=projection.metadata,
    )
    response = TestClient(app).post(
        "/v1/accounts", json={"account_id": "x"}, headers=_auth("alice")
    )
    assert response.status_code == 501
