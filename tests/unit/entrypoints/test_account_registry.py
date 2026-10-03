"""Registry semantics and containment of the old HTTP self-registration path."""

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
from cloudscale.domain.commands import Deposit
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
        "iat": datetime.now(UTC),
        "exp": datetime.now(UTC) + timedelta(minutes=5),
    }
    payload.update(claims)
    if payload.get("scope") == "accounts:admin" and "jti" not in payload:
        payload["jti"] = "test-jti-" + uuid.uuid4().hex
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
    app.state.test_registry = registry
    with TestClient(app) as test_client:
        yield test_client


@pytest.mark.parametrize(
    "claims", [{}, {"accounts": ["acct-registered"]}, {"scope": "accounts:admin"}]
)
@pytest.mark.parametrize("account", ["acct-new", "acct-registered", "acct-funded"])
def test_self_registration_denied_without_binding_or_event(
    tmp_path, claims: dict, account: str, caplog: pytest.LogCaptureFixture
) -> None:
    log = str(tmp_path / "log.db")
    uow = SqliteCommandUnitOfWork(log)
    projection = DeadLetteringProjectionStore(path=str(tmp_path / "projection.db"))
    registry = SqliteAccountRegistry(log)
    registry.register("acct-registered", "alice")
    if account == "acct-funded":
        CommandService(uow).execute(
            Deposit(account_id="acct-funded", expected_version=0, amount=100),
            command_id=uuid.uuid4(),
            issuer="cloudscale",
            subject="funding-service",
        )
    app = create_app(
        HttpSettings(jwt_secret=SECRET, rate_limit_per_minute=0),
        command_service=CommandService(uow),
        query_service=QueryService(StoreProjectionReader(projection)),
        storage_metadata=projection.metadata,
        account_registry=registry,
        closeables=(uow, projection, registry),
    )
    with TestClient(app) as http:
        before = {name: uow.fold_stream(name) for name in (account, "acct-registered")}
        with caplog.at_level(logging.INFO, logger="cloudscale.audit"):
            response = http.post(
                "/v1/accounts",
                json={"account_id": account},
                headers=_auth("bob", **claims),
            )
        assert response.status_code == 403
        assert response.json() == {
            "detail": "principal is not authorized for this account"
        }
        assert registry.owner_of(account) == (
            "alice" if account == "acct-registered" else None
        )
        assert {name: uow.fold_stream(name) for name in before} == before
        assert not any(r.getMessage() == "account.registered" for r in caplog.records)
        assert (
            http.post("/v1/accounts", json={"account_id": account}).status_code == 401
        )
        assert (
            http.post(
                "/v1/accounts",
                json={"account_id": account, "extra": 1},
                headers=_auth("bob"),
            ).status_code
            == 422
        )


def test_existing_registry_owner_access_survives_self_registration_denial(
    client: TestClient,
) -> None:
    alice, bob = _auth("alice"), _auth("bob")

    # Unregistered, unnamed account: denied for everyone without admin.
    assert (
        client.post(
            "/v1/accounts/acct-a/commands", json=_body(), headers=alice
        ).status_code
        == 403
    )

    assert (
        client.post(
            "/v1/accounts", json={"account_id": "acct-a"}, headers=alice
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/v1/accounts/acct-a/commands", json=_body(), headers=alice
        ).status_code
        == 403
    )

    # A pre-existing ownership binding still authorizes the owner.
    client.app.state.test_registry.register("acct-a", "alice")
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


def test_self_registration_always_denies_without_owner_disclosure(
    client: TestClient,
) -> None:
    alice, bob = _auth("alice"), _auth("bob")
    for headers in (alice, bob):
        denied = client.post(
            "/v1/accounts", json={"account_id": "acct-b"}, headers=headers
        )
        assert denied.status_code == 403
        assert denied.json() == {
            "detail": "principal is not authorized for this account"
        }


def test_claims_and_admin_still_grant_access_to_unregistered_stream(
    client: TestClient,
) -> None:
    # Claim/admin grants still authorize operations on unregistered streams.
    named = _auth("service", accounts=["acct-c"])
    assert (
        client.post(
            "/v1/accounts/acct-c/commands", json=_body(), headers=named
        ).status_code
        == 201
    )
    admin = _auth("ops", scope="accounts:admin")
    assert client.get("/v1/accounts/acct-c/balance", headers=admin).status_code == 404


def test_self_registration_requires_auth_and_rejects_bad_input(
    client: TestClient,
) -> None:
    assert client.post("/v1/accounts", json={"account_id": "x"}).status_code == 401
    alice = _auth("alice")
    assert (
        client.post("/v1/accounts", json={"account_id": ""}, headers=alice).status_code
        == 403
    )
    assert (
        client.post(
            "/v1/accounts", json={"account_id": "x", "extra": 1}, headers=alice
        ).status_code
        == 422
    )


def test_denied_registration_does_not_log_a_created_binding(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.INFO, logger="cloudscale.audit"):
        client.post(
            "/v1/accounts", json={"account_id": "acct-d"}, headers=_auth("alice")
        )
        client.post("/v1/accounts", json={"account_id": "acct-d"}, headers=_auth("bob"))
    records = [r for r in caplog.records if r.getMessage() == "account.registered"]
    assert records == []


def test_registration_endpoint_is_403_when_no_registry_is_wired(tmp_path) -> None:
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
    assert response.status_code == 403


@pytest.mark.parametrize("anchor_access", ["registry", "claims"])
def test_postings_accepts_registry_ownership_of_every_additional_debit(
    client: TestClient, anchor_access: str
) -> None:
    for account in ("acct-anchor", "acct-secondary"):
        client.app.state.test_registry.register(account, "alice")
        assert (
            client.post(
                f"/v1/accounts/{account}/commands",
                json=_body(),
                headers=_auth("alice", accounts=[account]),
            ).status_code
            == 201
        )
    headers = (
        _auth("alice")
        if anchor_access == "registry"
        else _auth("alice", accounts=["acct-anchor"])
    )
    body = {
        "command_id": str(uuid.uuid4()),
        "expected_version": 1,
        "postings": [
            {"account_id": "acct-anchor", "amount": 1, "direction": "debit"},
            {"account_id": "acct-secondary", "amount": 100, "direction": "debit"},
            {"account_id": "acct-recipient", "amount": 101, "direction": "credit"},
        ],
    }
    outsider = _auth("bob", accounts=["acct-anchor"])
    denied = client.post(
        "/v1/accounts/acct-anchor/postings", json=body, headers=outsider
    )
    assert denied.status_code == 403
    assert denied.json() == {"detail": "principal is not authorized for this account"}
    accepted = client.post(
        "/v1/accounts/acct-anchor/postings", json=body, headers=headers
    )
    assert accepted.status_code == 201, accepted.text
    assert {
        p["account_id"]: p["committed_version"] for p in accepted.json()["postings"]
    } == {"acct-anchor": 2, "acct-secondary": 2, "acct-recipient": None}
