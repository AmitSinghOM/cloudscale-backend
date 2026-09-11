"""OIDC/JWKS identity mode, token lifetime cap, admin jti revocation, settings.

A local RSA key pair stands in for the issuer; ``PyJWKClient.fetch_data`` is
pointed at an in-memory JWKS so rotation can be exercised deterministically.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from jwt import PyJWKClient
from jwt.algorithms import RSAAlgorithm

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
from cloudscale.entrypoints.http.verifiers import JwksVerifier

ISSUER = "https://issuer.example"
JWKS_URL = f"{ISSUER}/.well-known/jwks.json"


class _FakeIssuer:
    """Holds RSA keys and serves them as a JWKS the verifier can fetch."""

    def __init__(self) -> None:
        self.keys: dict[str, rsa.RSAPrivateKey] = {}
        self.fetches = 0

    def rotate(self, kid: str) -> None:
        self.keys[kid] = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    def jwks(self) -> dict:
        self.fetches += 1
        keys = []
        for kid, private in self.keys.items():
            jwk = RSAAlgorithm.to_jwk(private.public_key(), as_dict=True)
            jwk.update({"kid": kid, "use": "sig", "alg": "RS256"})
            keys.append(jwk)
        return {"keys": keys}

    def token(self, kid: str, **claims: object) -> str:
        payload: dict = {
            "iss": ISSUER,
            "sub": "alice",
            "iat": datetime.now(UTC),
            "exp": datetime.now(UTC) + timedelta(minutes=5),
            "scope": "accounts:admin",
            "jti": uuid.uuid4().hex,
        }
        payload.update(claims)
        payload = {k: v for k, v in payload.items() if v is not None}
        return jwt.encode(
            payload, self.keys[kid], algorithm="RS256", headers={"kid": kid}
        )

    def client(self) -> PyJWKClient:
        jwk_client = PyJWKClient(JWKS_URL, cache_keys=True, lifespan=300)
        jwk_client.fetch_data = self.jwks  # type: ignore[method-assign]
        return jwk_client


def _app(tmp_path, settings: HttpSettings, verifier=None):
    uow = SqliteCommandUnitOfWork(str(tmp_path / "log.db"))
    projection = DeadLetteringProjectionStore(path=str(tmp_path / "p.db"))
    return create_app(
        settings,
        command_service=CommandService(uow),
        query_service=QueryService(StoreProjectionReader(projection)),
        storage_metadata=projection.metadata,
        token_verifier=verifier,
    )


@pytest.fixture()
def issuer() -> _FakeIssuer:
    fake = _FakeIssuer()
    fake.rotate("key-1")
    return fake


@pytest.fixture()
def jwks_client(tmp_path, issuer: _FakeIssuer) -> TestClient:
    settings = HttpSettings(
        jwt_jwks_url=JWKS_URL, jwt_issuer=ISSUER, rate_limit_per_minute=0
    )
    verifier = JwksVerifier(
        JWKS_URL, settings.jwt_algorithms, ISSUER, jwk_client=issuer.client()
    )
    return TestClient(_app(tmp_path, settings, verifier))


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# -- JWKS mode ---------------------------------------------------------------------


def test_rs256_token_from_issuer_is_accepted(jwks_client: TestClient, issuer) -> None:
    response = jwks_client.get(
        "/v1/accounts/x/balance", headers=_bearer(issuer.token("key-1"))
    )
    assert response.status_code == 404  # authenticated + authorized; no account


def test_key_rotation_is_picked_up_via_kid(jwks_client: TestClient, issuer) -> None:
    assert (
        jwks_client.get(
            "/v1/accounts/x/balance", headers=_bearer(issuer.token("key-1"))
        ).status_code
        == 404
    )
    fetches_before = issuer.fetches

    issuer.rotate("key-2")  # issuer publishes a new key
    response = jwks_client.get(
        "/v1/accounts/x/balance", headers=_bearer(issuer.token("key-2"))
    )
    assert response.status_code == 404
    assert issuer.fetches > fetches_before  # unknown kid forced a JWKS refetch


def test_token_signed_by_unknown_key_is_rejected(jwks_client: TestClient) -> None:
    rogue = _FakeIssuer()
    rogue.rotate("key-1")  # same kid, different key material
    response = jwks_client.get(
        "/v1/accounts/x/balance", headers=_bearer(rogue.token("key-1"))
    )
    assert response.status_code == 401


def test_hmac_token_is_rejected_in_jwks_mode(jwks_client: TestClient) -> None:
    """Algorithm confusion: an HS256 token must never verify against JWKS."""
    forged = jwt.encode(
        {
            "iss": ISSUER,
            "sub": "alice",
            "iat": datetime.now(UTC),
            "exp": datetime.now(UTC) + timedelta(minutes=5),
            "scope": "accounts:admin",
            "jti": "x",
        },
        "any-shared-secret-0123456789abcdef-0123456789",
        algorithm="HS256",
        headers={"kid": "key-1"},
    )
    assert (
        jwks_client.get("/v1/accounts/x/balance", headers=_bearer(forged)).status_code
        == 401
    )


# -- lifetime and revocation (mode-independent) -----------------------------------


def test_tokens_longer_than_max_lifetime_are_rejected(
    jwks_client: TestClient, issuer
) -> None:
    long_lived = issuer.token(
        "key-1",
        exp=datetime.now(UTC) + timedelta(hours=2),  # default max 1h
    )
    assert (
        jwks_client.get(
            "/v1/accounts/x/balance", headers=_bearer(long_lived)
        ).status_code
        == 401
    )


def test_tokens_without_iat_are_rejected(tmp_path, issuer) -> None:
    settings = HttpSettings(
        jwt_jwks_url=JWKS_URL, jwt_issuer=ISSUER, rate_limit_per_minute=0
    )
    verifier = JwksVerifier(
        JWKS_URL, settings.jwt_algorithms, ISSUER, jwk_client=issuer.client()
    )
    client = TestClient(_app(tmp_path, settings, verifier))
    payload = {
        "iss": ISSUER,
        "sub": "alice",
        "exp": datetime.now(UTC) + timedelta(minutes=5),
    }
    token = jwt.encode(
        payload, issuer.keys["key-1"], algorithm="RS256", headers={"kid": "key-1"}
    )
    assert (
        client.get("/v1/accounts/x/balance", headers=_bearer(token)).status_code == 401
    )


def test_admin_tokens_need_jti_and_revoked_jtis_are_refused(tmp_path, issuer) -> None:
    settings = HttpSettings(
        jwt_jwks_url=JWKS_URL,
        jwt_issuer=ISSUER,
        jwt_revoked_jtis=["revoked-1"],
        rate_limit_per_minute=0,
    )
    verifier = JwksVerifier(
        JWKS_URL, settings.jwt_algorithms, ISSUER, jwk_client=issuer.client()
    )
    client = TestClient(_app(tmp_path, settings, verifier))

    payload_no_jti = {
        "iss": ISSUER,
        "sub": "ops",
        "iat": datetime.now(UTC),
        "exp": datetime.now(UTC) + timedelta(minutes=5),
        "scope": "accounts:admin",
    }
    no_jti = jwt.encode(
        payload_no_jti,
        issuer.keys["key-1"],
        algorithm="RS256",
        headers={"kid": "key-1"},
    )
    assert (
        client.get("/v1/accounts/x/balance", headers=_bearer(no_jti)).status_code == 401
    )

    revoked = issuer.token("key-1", jti="revoked-1")
    assert (
        client.get("/v1/accounts/x/balance", headers=_bearer(revoked)).status_code
        == 401
    )

    live = issuer.token("key-1", jti="live-1")
    assert (
        client.get("/v1/accounts/x/balance", headers=_bearer(live)).status_code == 404
    )

    # Non-admin tokens do not need a jti at all.
    user = issuer.token("key-1", scope="", accounts=["x"], jti=None)
    assert (
        client.get("/v1/accounts/x/balance", headers=_bearer(user)).status_code == 404
    )


# -- settings are fail-closed --------------------------------------------------------


def test_settings_require_exactly_one_identity_mode() -> None:
    with pytest.raises(Exception, match="exactly one"):
        HttpSettings()
    with pytest.raises(Exception, match="exactly one"):
        HttpSettings(jwt_secret="s" * 32, jwt_jwks_url=JWKS_URL)


def test_settings_reject_algorithm_confusion_at_config_time() -> None:
    with pytest.raises(Exception, match="not valid for identity mode"):
        HttpSettings(jwt_jwks_url=JWKS_URL, jwt_algorithms=["HS256"])
    with pytest.raises(Exception, match="not valid for identity mode"):
        HttpSettings(jwt_secret="s" * 32, jwt_algorithms=["RS256"])
    assert HttpSettings(jwt_jwks_url=JWKS_URL).jwt_algorithms == ["RS256", "ES256"]
    assert HttpSettings(jwt_secret="s" * 32).jwt_algorithms == ["HS256"]
