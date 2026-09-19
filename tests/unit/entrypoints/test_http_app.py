"""HTTP tier contracts: auth is fail-closed, health discloses storage tier,
the query endpoint speaks through the typed application layer."""

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
    scope: str | None = "accounts:admin",
    accounts: list[str] | None = None,
) -> str:
    claims: dict = {
        "iss": issuer,
        "iat": datetime.now(UTC),
        "exp": datetime.now(UTC) + timedelta(seconds=expires_in),
    }
    if subject is not None:
        claims["sub"] = subject
    if scope is not None:
        claims["scope"] = scope
        if scope == "accounts:admin":
            claims["jti"] = "test-jti-" + uuid.uuid4().hex
    if accounts is not None:
        claims["accounts"] = accounts
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
        "held": 0,
        "available": 75,
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
    assert replay.status_code == 201  # identical stored response, status included
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
        "held": 0,
        "available": 380,
        "version": 2,
        "consistency": "eventual",
    }


def test_future_schema_event_in_stream_maps_to_503_not_500(
    stack: _Stack, caplog
) -> None:
    """A rolled-back deploy leaves newer-schema events in the log. The command
    path must say 'retry later' (503), never leak a 500 (ADR-0009, RUNBOOK R1),
    and must tell operators WHY -- the request log and metrics only see 503."""
    import logging
    import sqlite3

    caplog.set_level(logging.ERROR, logger="cloudscale.http")

    conn = sqlite3.connect(stack.log_path)
    conn.execute(
        "INSERT INTO events (event_id, stream, seq, type, account_id, amount, schema_version)"
        " VALUES ('future-1', 'account-acct-9', 1, 'Deposited', 'acct-9', 5, 99)"
    )
    conn.commit()
    conn.close()

    response = stack.client.post(
        "/v1/accounts/acct-9/commands", json=_command_body(), headers=_auth()
    )
    assert response.status_code == 503
    assert "newer than this build" in response.json()["detail"]
    assert int(response.headers["Retry-After"]) >= 1
    [record] = [r for r in caplog.records if "schema newer" in r.getMessage()]
    assert record.levelno == logging.ERROR
    assert record.account_id == "acct-9"  # type: ignore[attr-defined]


def test_public_dev_secret_in_production_mode_is_warned_not_refused() -> None:
    """Review finding: compose.yaml ships a public secret in migrations mode. The
    server must start (the quickstart depends on it) but must say so loudly."""
    from cloudscale.entrypoints.http.settings import (
        KNOWN_DEVELOPMENT_SECRETS,
        HttpSettings,
    )

    dev_secret = next(iter(KNOWN_DEVELOPMENT_SECRETS))
    settings = HttpSettings(jwt_secret=dev_secret)
    assert settings.production_warnings(schema_mode="auto") == []  # dev: silent
    [warning] = settings.production_warnings(schema_mode="migrations")
    assert "PUBLIC development secret" in warning and "RUNBOOK D4" in warning
    assert (
        HttpSettings(jwt_secret=SECRET).production_warnings(schema_mode="migrations")
        == []
    )


# -- double-entry transfers (ADR-0011) ------------------------------------------------


def _transfer_body(**overrides: object) -> dict:
    body: dict = {
        "command_id": str(uuid.uuid4()),
        "target_account_id": "acct-dst",
        "amount": 40,
        "expected_version": 1,
    }
    body.update(overrides)
    return body


def _seed(stack: _Stack, account: str, amount: int = 100) -> None:
    response = stack.client.post(
        f"/v1/accounts/{account}/commands",
        json=_command_body(amount=amount),
        headers=_auth(),
    )
    assert response.status_code == 201


def test_transfer_returns_both_postings_and_the_source_fields(stack: _Stack) -> None:
    _seed(stack, "acct-src")
    body = _transfer_body()
    response = stack.client.post(
        "/v1/accounts/acct-src/transfers", json=body, headers=_auth()
    )
    assert response.status_code == 201
    result = response.json()
    assert result["outcome"] == "accepted"
    assert result["account_id"] == "acct-src"
    assert result["committed_version"] == 2
    postings = {p["account_id"]: p for p in result["postings"]}
    assert set(postings) == {"acct-src", "acct-dst"}
    assert postings["acct-src"]["committed_version"] == 2
    assert postings["acct-dst"]["committed_version"] == 1  # admin may read both
    assert postings["acct-src"]["event_id"] == result["event_id"]

    replay = stack.client.post(
        "/v1/accounts/acct-src/transfers", json=body, headers=_auth()
    )
    assert replay.status_code == 201  # replay is the stored response, byte for byte
    assert replay.json() == result


def test_transfer_authorizes_the_source_only_and_redacts_the_target_version(
    stack: _Stack,
) -> None:
    _seed(stack, "acct-src")
    owner_only = {
        "Authorization": f"Bearer {_token(scope=None, accounts=['acct-src'])}"
    }
    response = stack.client.post(
        "/v1/accounts/acct-src/transfers", json=_transfer_body(), headers=owner_only
    )
    assert response.status_code == 201
    postings = {p["account_id"]: p for p in response.json()["postings"]}
    assert postings["acct-src"]["committed_version"] == 2
    # The target is not the caller's account: its activity count is not disclosed.
    assert postings["acct-dst"]["committed_version"] is None
    assert postings["acct-dst"]["event_id"]

    # A token for another account cannot move money out of acct-src.
    stranger = {"Authorization": f"Bearer {_token(scope=None, accounts=['acct-dst'])}"}
    denied = stack.client.post(
        "/v1/accounts/acct-src/transfers",
        json=_transfer_body(expected_version=2),
        headers=stranger,
    )
    assert denied.status_code == 403


def test_transfer_rejections_map_to_documented_statuses(stack: _Stack) -> None:
    _seed(stack, "acct-src", amount=10)
    same = stack.client.post(
        "/v1/accounts/acct-src/transfers",
        json=_transfer_body(target_account_id="acct-src"),
        headers=_auth(),
    )
    assert same.status_code == 400
    assert same.json()["detail"] == "same_account"

    short = stack.client.post(
        "/v1/accounts/acct-src/transfers",
        json=_transfer_body(amount=11),
        headers=_auth(),
    )
    assert short.status_code == 422
    assert short.json()["error_code"] == "insufficient_funds"
    assert short.json()["postings"] == []

    stale = stack.client.post(
        "/v1/accounts/acct-src/transfers",
        json=_transfer_body(amount=1, expected_version=0),
        headers=_auth(),
    )
    assert stale.status_code == 409
    assert stale.json()["error_code"] == "version_conflict"
    assert stale.json()["current_version"] == 1


def test_transfer_reaches_both_balances_through_the_consumer(
    stack: _Stack, caplog
) -> None:
    _seed(stack, "acct-src")
    caplog.set_level(logging.INFO, logger="cloudscale.audit")
    accepted = stack.client.post(
        "/v1/accounts/acct-src/transfers", json=_transfer_body(), headers=_auth()
    )
    assert accepted.status_code == 201
    # The audit record names where the money went, not only where it left.
    [record] = [
        r for r in caplog.records if getattr(r, "command_type", None) == "transfer"
    ]
    assert {p["account_id"] for p in record.postings} == {"acct-src", "acct-dst"}
    stack.run_consumer()
    src = stack.client.get("/v1/accounts/acct-src/balance", headers=_auth()).json()
    dst = stack.client.get("/v1/accounts/acct-dst/balance", headers=_auth()).json()
    assert (src["balance"], src["version"]) == (60, 2)
    assert (dst["balance"], dst["version"]) == (40, 1)


# -- N-leg postings (ADR-0013) ---------------------------------------------------------


def _postings_body(**overrides: object) -> dict:
    body: dict = {
        "command_id": str(uuid.uuid4()),
        "postings": [
            {"account_id": "acct-src", "amount": 100, "direction": "debit"},
            {"account_id": "acct-merchant", "amount": 97, "direction": "credit"},
            {"account_id": "acct-fees", "amount": 3, "direction": "credit"},
        ],
        "expected_version": 1,
    }
    body.update(overrides)
    return body


def test_post_postings_returns_one_posting_per_leg_and_reaches_every_balance(
    stack: _Stack, caplog
) -> None:
    _seed(stack, "acct-src")
    caplog.set_level(logging.INFO, logger="cloudscale.audit")
    body = _postings_body()
    response = stack.client.post(
        "/v1/accounts/acct-src/postings", json=body, headers=_auth()
    )
    assert response.status_code == 201, response.text
    result = response.json()
    assert result["outcome"] == "accepted"
    assert result["account_id"] == "acct-src" and result["committed_version"] == 2
    postings = {p["account_id"]: p for p in result["postings"]}
    assert set(postings) == {"acct-src", "acct-merchant", "acct-fees"}
    assert postings["acct-src"]["committed_version"] == 2
    assert postings["acct-merchant"]["committed_version"] == 1
    [record] = [r for r in caplog.records if getattr(r, "command_type", None) == "post"]
    assert {p["account_id"] for p in record.postings} == set(postings)

    replay = stack.client.post(
        "/v1/accounts/acct-src/postings", json=body, headers=_auth()
    )
    assert replay.status_code == 201 and replay.json() == result

    stack.run_consumer()
    balances = {
        name: stack.client.get(f"/v1/accounts/{name}/balance", headers=_auth()).json()
        for name in ("acct-src", "acct-merchant", "acct-fees")
    }
    assert balances["acct-src"]["balance"] == 0
    assert balances["acct-merchant"]["balance"] == 97
    assert balances["acct-fees"]["balance"] == 3


def test_post_postings_authorizes_the_anchor_and_redacts_other_legs(
    stack: _Stack,
) -> None:
    _seed(stack, "acct-src")
    owner_only = {
        "Authorization": f"Bearer {_token(scope=None, accounts=['acct-src'])}"
    }
    response = stack.client.post(
        "/v1/accounts/acct-src/postings", json=_postings_body(), headers=owner_only
    )
    assert response.status_code == 201
    postings = {p["account_id"]: p for p in response.json()["postings"]}
    assert postings["acct-src"]["committed_version"] == 2
    assert postings["acct-merchant"]["committed_version"] is None
    assert postings["acct-fees"]["committed_version"] is None

    # Owning the merchant account does not let you anchor on acct-src.
    stranger = {
        "Authorization": f"Bearer {_token(scope=None, accounts=['acct-merchant'])}"
    }
    denied = stack.client.post(
        "/v1/accounts/acct-src/postings",
        json=_postings_body(expected_version=2),
        headers=stranger,
    )
    assert denied.status_code == 403


@pytest.mark.parametrize(
    "legs, code",
    [
        (
            [
                {"account_id": "acct-src", "amount": 100, "direction": "debit"},
                {"account_id": "acct-m", "amount": 99, "direction": "credit"},
            ],
            "unbalanced",
        ),
        (
            [
                {"account_id": "acct-src", "amount": 50, "direction": "debit"},
                {"account_id": "acct-src", "amount": 50, "direction": "credit"},
            ],
            "duplicate_account",
        ),
        (
            [
                {"account_id": "acct-m", "amount": 10, "direction": "debit"},
                {"account_id": "acct-src", "amount": 10, "direction": "credit"},
            ],
            "anchor_not_debited",
        ),
        (
            [{"account_id": "acct-src", "amount": 17, "direction": "debit"}]
            + [
                {"account_id": f"acct-c{i}", "amount": 1, "direction": "credit"}
                for i in range(17)
            ],
            "too_many_legs",
        ),
    ],
)
def test_post_postings_invalid_sets_are_400_with_the_domain_code(
    stack: _Stack, legs: list, code: str
) -> None:
    _seed(stack, "acct-src")
    response = stack.client.post(
        "/v1/accounts/acct-src/postings",
        json=_postings_body(postings=legs),
        headers=_auth(),
    )
    assert response.status_code == 400, response.text
    assert response.json()["detail"] == code


def test_post_postings_business_rejections_map_to_documented_statuses(
    stack: _Stack,
) -> None:
    _seed(stack, "acct-src", amount=10)
    short = stack.client.post(
        "/v1/accounts/acct-src/postings",
        json=_postings_body(
            postings=[
                {"account_id": "acct-src", "amount": 11, "direction": "debit"},
                {"account_id": "acct-m", "amount": 11, "direction": "credit"},
            ]
        ),
        headers=_auth(),
    )
    assert short.status_code == 422
    assert short.json()["error_code"] == "insufficient_funds"
    assert short.json()["postings"] == []

    stale = stack.client.post(
        "/v1/accounts/acct-src/postings",
        json=_postings_body(
            expected_version=0,
            postings=[
                {"account_id": "acct-src", "amount": 1, "direction": "debit"},
                {"account_id": "acct-m", "amount": 1, "direction": "credit"},
            ],
        ),
        headers=_auth(),
    )
    assert stale.status_code == 409
    assert stale.json()["error_code"] == "version_conflict"

    bad_direction = stack.client.post(
        "/v1/accounts/acct-src/postings",
        json=_postings_body(
            postings=[
                {"account_id": "acct-src", "amount": 1, "direction": "sideways"},
                {"account_id": "acct-m", "amount": 1, "direction": "credit"},
            ]
        ),
        headers=_auth(),
    )
    assert bad_direction.status_code == 422  # request-shape validation


# -- holds (ADR-0014) ------------------------------------------------------------------


def _hold_body(**overrides: object) -> dict:
    body: dict = {
        "command_id": str(uuid.uuid4()),
        "target_account_id": "acct-dst",
        "amount": 40,
        "expected_version": 1,
        "ttl_seconds": 3600,
    }
    body.update(overrides)
    return body


def test_hold_lifecycle_over_http_place_post_and_balances(stack: _Stack) -> None:
    _seed(stack, "acct-src")
    hold = _hold_body()
    placed = stack.client.post(
        "/v1/accounts/acct-src/holds", json=hold, headers=_auth()
    )
    assert placed.status_code == 201, placed.text
    assert placed.json()["committed_version"] == 2
    assert [p["account_id"] for p in placed.json()["postings"]] == ["acct-src"]

    # The retry rule the whole API rests on: the same command_id is a replay,
    # byte for byte -- even though the decision stamps expires_at from its
    # clock (found by the independent review; this was a 409 before the fix).
    replay = stack.client.post(
        "/v1/accounts/acct-src/holds", json=hold, headers=_auth()
    )
    assert replay.status_code == 201
    assert replay.json() == placed.json()
    # A different TTL under the same id is a different intent: conflict.
    changed = stack.client.post(
        "/v1/accounts/acct-src/holds",
        json={**hold, "ttl_seconds": 7200},
        headers=_auth(),
    )
    assert changed.status_code == 409
    assert changed.json()["error_code"] == "command_id_conflict"

    stack.run_consumer()
    src = stack.client.get("/v1/accounts/acct-src/balance", headers=_auth()).json()
    assert (src["balance"], src["held"], src["available"]) == (100, 40, 60)
    # The target has no stream yet: a hold reveals nothing to it.
    assert (
        stack.client.get("/v1/accounts/acct-dst/balance", headers=_auth()).status_code
        == 404
    )

    # Available, not balance, bounds a withdraw.
    short = stack.client.post(
        "/v1/accounts/acct-src/commands",
        json={
            "command_id": str(uuid.uuid4()),
            "type": "withdraw",
            "amount": 61,
            "expected_version": 2,
        },
        headers=_auth(),
    )
    assert short.status_code == 422

    posted = stack.client.post(
        f"/v1/accounts/acct-src/holds/{hold['command_id']}/post",
        json={"command_id": str(uuid.uuid4()), "expected_version": 2, "amount": 25},
        headers=_auth(),
    )
    assert posted.status_code == 201, posted.text
    postings = {
        p["account_id"]: p["committed_version"] for p in posted.json()["postings"]
    }
    assert postings == {"acct-src": 4, "acct-dst": 1}  # partial: post + release on src
    stack.run_consumer()
    src = stack.client.get("/v1/accounts/acct-src/balance", headers=_auth()).json()
    dst = stack.client.get("/v1/accounts/acct-dst/balance", headers=_auth()).json()
    assert (src["balance"], src["held"], src["available"]) == (75, 0, 75)
    assert dst["balance"] == 25
    assert src["balance"] + dst["balance"] == 100


def _placed_hold(stack: _Stack) -> dict:
    _seed(stack, "acct-src")
    hold = _hold_body()
    placed = stack.client.post(
        "/v1/accounts/acct-src/holds", json=hold, headers=_auth()
    )
    assert placed.status_code == 201, placed.text
    return hold


def _post_json(stack: _Stack, path: str, body: dict):
    return stack.client.post(path, json=body, headers=_auth())


def test_hold_void_over_http_then_post_is_hold_not_open(stack: _Stack) -> None:
    hold = _placed_hold(stack)
    base = f"/v1/accounts/acct-src/holds/{hold['command_id']}"
    voided = _post_json(
        stack, f"{base}/void", {"command_id": str(uuid.uuid4()), "expected_version": 2}
    )
    assert voided.status_code == 201, voided.text
    gone = _post_json(
        stack, f"{base}/post", {"command_id": str(uuid.uuid4()), "expected_version": 3}
    )
    assert gone.status_code == 400 and gone.json()["error_code"] == "hold_not_open"
    unknown = _post_json(
        stack,
        f"/v1/accounts/acct-src/holds/{uuid.uuid4()}/void",
        {"command_id": str(uuid.uuid4()), "expected_version": 3},
    )
    assert (
        unknown.status_code == 400 and unknown.json()["error_code"] == "hold_not_open"
    )


def test_hold_rejections_over_http(stack: _Stack) -> None:
    hold = _placed_hold(stack)
    too_much = _post_json(
        stack,
        f"/v1/accounts/acct-src/holds/{hold['command_id']}/post",
        {"command_id": str(uuid.uuid4()), "expected_version": 2, "amount": 41},
    )
    assert too_much.status_code == 400
    assert too_much.json()["error_code"] == "capture_exceeds_hold"

    holds = "/v1/accounts/acct-src/holds"
    over_available = _post_json(
        stack, holds, _hold_body(amount=101, expected_version=2)
    )
    assert over_available.status_code == 422
    assert over_available.json()["error_code"] == "insufficient_funds"

    same = _post_json(
        stack, holds, _hold_body(target_account_id="acct-src", expected_version=2)
    )
    assert same.status_code == 400 and same.json()["detail"] == "same_account"

    ttl = _post_json(stack, holds, _hold_body(ttl_seconds=10**9, expected_version=2))
    assert ttl.status_code == 400
    assert "CLOUDSCALE_HOLD_MAX_TTL_SECONDS" in ttl.json()["detail"]


def test_hold_routes_authorize_the_source_and_redact_the_target(stack: _Stack) -> None:
    _seed(stack, "acct-src")
    owner_only = {
        "Authorization": f"Bearer {_token(scope=None, accounts=['acct-src'])}"
    }
    hold = _hold_body()
    assert (
        stack.client.post(
            "/v1/accounts/acct-src/holds", json=hold, headers=owner_only
        ).status_code
        == 201
    )
    posted = stack.client.post(
        f"/v1/accounts/acct-src/holds/{hold['command_id']}/post",
        json={"command_id": str(uuid.uuid4()), "expected_version": 2},
        headers=owner_only,
    )
    assert posted.status_code == 201
    postings = {
        p["account_id"]: p["committed_version"] for p in posted.json()["postings"]
    }
    assert postings == {"acct-src": 3, "acct-dst": None}  # target redacted

    stranger = {"Authorization": f"Bearer {_token(scope=None, accounts=['acct-dst'])}"}
    denied_hold = stack.client.post(
        "/v1/accounts/acct-src/holds",
        json=_hold_body(expected_version=3),
        headers=stranger,
    )
    assert denied_hold.status_code == 403
    denied_void = stack.client.post(
        f"/v1/accounts/acct-src/holds/{uuid.uuid4()}/void",
        json={"command_id": str(uuid.uuid4()), "expected_version": 3},
        headers=stranger,
    )
    assert denied_void.status_code == 403


# -- reverts (ADR-0015) ----------------------------------------------------------------


def _committed_transfer_http(stack: _Stack) -> str:
    _seed(stack, "acct-src")
    body = _transfer_body()
    assert (
        stack.client.post(
            "/v1/accounts/acct-src/transfers", json=body, headers=_auth()
        ).status_code
        == 201
    )
    return body["command_id"]


def test_revert_appends_the_mirror_and_shows_in_the_transfers_read_model(
    stack: _Stack, caplog
) -> None:
    transfer_id = _committed_transfer_http(stack)
    caplog.set_level(logging.INFO, logger="cloudscale.audit")
    body = {"command_id": str(uuid.uuid4()), "expected_version": 2}
    response = stack.client.post(
        f"/v1/accounts/acct-src/transfers/{transfer_id}/revert",
        json=body,
        headers=_auth(),
    )
    assert response.status_code == 201, response.text
    result = response.json()
    assert {p["account_id"]: p["committed_version"] for p in result["postings"]} == {
        "acct-src": 3,
        "acct-dst": 2,
    }
    [record] = [
        r for r in caplog.records if getattr(r, "command_type", None) == "revert"
    ]
    assert {p["account_id"] for p in record.postings} == {"acct-src", "acct-dst"}

    replay = stack.client.post(
        f"/v1/accounts/acct-src/transfers/{transfer_id}/revert",
        json=body,
        headers=_auth(),
    )
    assert replay.status_code == 201 and replay.json() == result

    again = stack.client.post(
        f"/v1/accounts/acct-src/transfers/{transfer_id}/revert",
        json={"command_id": str(uuid.uuid4()), "expected_version": 3},
        headers=_auth(),
    )
    assert again.status_code == 409 and again.json()["error_code"] == "already_reverted"

    stack.run_consumer()
    src = stack.client.get("/v1/accounts/acct-src/balance", headers=_auth()).json()
    dst = stack.client.get("/v1/accounts/acct-dst/balance", headers=_auth()).json()
    assert (src["balance"], dst["balance"]) == (100, 0)
    view = stack.client.get(f"/v1/transfers/{transfer_id}", headers=_auth())
    assert view.status_code == 200, view.text
    assert view.json()["kind"] == "transfer"
    assert view.json()["reverted_by"] == body["command_id"]
    assert {
        (leg["account_id"], leg["direction"], leg["amount"])
        for leg in view.json()["legs"]
    } == {
        ("acct-src", "debit", 40),
        ("acct-dst", "credit", 40),
    }
    reversal = stack.client.get(
        f"/v1/transfers/{body['command_id']}", headers=_auth()
    ).json()
    assert reversal["kind"] == "reversal" and reversal["reverts"] == transfer_id


def test_revert_authorizes_on_the_accounts_it_debits_not_the_anchor(
    stack: _Stack,
) -> None:
    transfer_id = _committed_transfer_http(stack)
    body = {"command_id": str(uuid.uuid4()), "expected_version": 2}
    payer_only = {
        "Authorization": f"Bearer {_token(scope=None, accounts=['acct-src'])}"
    }
    denied = stack.client.post(
        f"/v1/accounts/acct-src/transfers/{transfer_id}/revert",
        json=body,
        headers=payer_only,
    )
    assert denied.status_code == 403  # the payer alone cannot claw the payment back
    # Not persisted: the same command_id succeeds when the payee consents.
    both = {
        "Authorization": f"Bearer {_token(scope=None, accounts=['acct-src', 'acct-dst'])}"
    }
    allowed = stack.client.post(
        f"/v1/accounts/acct-src/transfers/{transfer_id}/revert", json=body, headers=both
    )
    assert allowed.status_code == 201, allowed.text
    # The payee sees its own amount and the payer's leg redacted.
    stack.run_consumer()
    payee_only = {
        "Authorization": f"Bearer {_token(scope=None, accounts=['acct-dst'])}"
    }
    view = stack.client.get(f"/v1/transfers/{transfer_id}", headers=payee_only).json()
    amounts = {leg["account_id"]: leg["amount"] for leg in view["legs"]}
    assert amounts == {"acct-dst": 40, "acct-src": None}


def test_revert_rejections_and_transfer_read_visibility(stack: _Stack) -> None:
    transfer_id = _committed_transfer_http(stack)
    # Anchor must be credited by the revert: anchoring on the payee is refused.
    wrong_anchor = stack.client.post(
        f"/v1/accounts/acct-dst/transfers/{transfer_id}/revert",
        json={"command_id": str(uuid.uuid4()), "expected_version": 1},
        headers=_auth(),
    )
    assert wrong_anchor.status_code == 400
    assert wrong_anchor.json()["error_code"] == "anchor_not_credited"
    # A deposit is not a posting set.
    not_set = stack.client.post(
        f"/v1/accounts/acct-src/transfers/{uuid.uuid4()}/revert",
        json={"command_id": str(uuid.uuid4()), "expected_version": 2},
        headers=_auth(),
    )
    assert (
        not_set.status_code == 400 and not_set.json()["error_code"] == "not_revertible"
    )
    # Payee spent the money: 422, nothing appended.
    stack.client.post(
        "/v1/accounts/acct-dst/commands",
        json={
            "command_id": str(uuid.uuid4()),
            "type": "withdraw",
            "amount": 1,
            "expected_version": 1,
        },
        headers=_auth(),
    )
    short = stack.client.post(
        f"/v1/accounts/acct-src/transfers/{transfer_id}/revert",
        json={"command_id": str(uuid.uuid4()), "expected_version": 2},
        headers=_auth(),
    )
    assert (
        short.status_code == 422 and short.json()["error_code"] == "insufficient_funds"
    )
    # A stranger may read none of the legs: 404, not 403 (existence undisclosed).
    stack.run_consumer()
    stranger = {
        "Authorization": f"Bearer {_token(scope=None, accounts=['acct-other'])}"
    }
    assert (
        stack.client.get(f"/v1/transfers/{transfer_id}", headers=stranger).status_code
        == 404
    )
    assert (
        stack.client.get(f"/v1/transfers/{uuid.uuid4()}", headers=_auth()).status_code
        == 404
    )


@pytest.mark.parametrize(
    "src, dst", [("acct-a-src", "acct-b-dst"), ("acct-z-src", "acct-b-dst")]
)
def test_posted_hold_kind_is_hold_posting_regardless_of_leg_order(
    stack: _Stack, src: str, dst: str
) -> None:
    """Legs are appended in account-id order, so the consumer sees HoldPosted first
    when src < dst and TransferCredited first otherwise; the kind must not depend
    on that (found by the independent review of ADR-0015)."""
    _seed(stack, src)
    hold = _hold_body(target_account_id=dst)
    assert (
        stack.client.post(
            f"/v1/accounts/{src}/holds", json=hold, headers=_auth()
        ).status_code
        == 201
    )
    posted = stack.client.post(
        f"/v1/accounts/{src}/holds/{hold['command_id']}/post",
        json={"command_id": str(uuid.uuid4()), "expected_version": 2},
        headers=_auth(),
    )
    assert posted.status_code == 201, posted.text
    stack.run_consumer()
    view = stack.client.get(
        f"/v1/transfers/{hold['command_id']}", headers=_auth()
    ).json()
    assert view["kind"] == "hold_posting", view
    assert {(leg["account_id"], leg["direction"]) for leg in view["legs"]} == {
        (src, "debit"),
        (dst, "credit"),
    }


def test_revert_route_is_not_an_existence_oracle_for_non_admins(stack: _Stack) -> None:
    """An anchor-only token gets the same 403 for a foreign set and a non-existent one."""
    transfer_id = _committed_transfer_http(stack)  # acct-src -> acct-dst, admin-made
    _seed(stack, "acct-other")
    other_only = {
        "Authorization": f"Bearer {_token(scope=None, accounts=['acct-other'])}"
    }
    body = {"command_id": str(uuid.uuid4()), "expected_version": 1}
    foreign = stack.client.post(
        f"/v1/accounts/acct-other/transfers/{transfer_id}/revert",
        json=body,
        headers=other_only,
    )
    missing = stack.client.post(
        f"/v1/accounts/acct-other/transfers/{uuid.uuid4()}/revert",
        json=body,
        headers=other_only,
    )
    assert foreign.status_code == missing.status_code == 403
    assert foreign.json() == missing.json()
    # An admin still gets the informative not_revertible for a missing id.
    admin = stack.client.post(
        f"/v1/accounts/acct-other/transfers/{uuid.uuid4()}/revert",
        json=body,
        headers=_auth(),
    )
    assert admin.status_code == 400 and admin.json()["error_code"] == "not_revertible"
