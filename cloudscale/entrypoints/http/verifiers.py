"""Token verification strategies: shared-secret HMAC and OIDC/JWKS.

Both return validated claims or raise ``jwt.InvalidTokenError``; the
authentication dependency maps every failure to one fixed 401. Algorithm
lists are pinned per mode at configuration time, so a token cannot talk the
verifier into a different algorithm family (``alg`` confusion).

``JwksVerifier`` resolves the signing key by ``kid`` through
``jwt.PyJWKClient``, which caches keys and refetches the JWKS when it meets
an unknown ``kid`` — that is how issuer key rotation works without restarts.
"""

from __future__ import annotations

from typing import Any, Protocol

import jwt
from jwt import PyJWKClient

from cloudscale.entrypoints.http.settings import HttpSettings


class TokenVerifier(Protocol):
    def verify(
        self, token: str, *, required_claims: list[str], audience: str | None
    ) -> dict[str, Any]: ...


def _decode(
    token: str,
    key: Any,
    *,
    algorithms: list[str],
    issuer: str,
    required_claims: list[str],
    audience: str | None,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if audience is not None:
        kwargs["audience"] = audience
    claims: dict[str, Any] = jwt.decode(
        token,
        key,
        algorithms=algorithms,
        issuer=issuer,
        options={"require": required_claims},
        **kwargs,
    )
    return claims


class HmacVerifier:
    """Shared-secret verification (HS256 family)."""

    def __init__(self, secret: str, algorithms: list[str], issuer: str) -> None:
        self._secret = secret
        self._algorithms = list(algorithms)
        self._issuer = issuer

    def verify(
        self, token: str, *, required_claims: list[str], audience: str | None
    ) -> dict[str, Any]:
        return _decode(
            token,
            self._secret,
            algorithms=self._algorithms,
            issuer=self._issuer,
            required_claims=required_claims,
            audience=audience,
        )


class JwksVerifier:
    """OIDC/JWKS verification (RS256 / ES256 families) with kid rotation."""

    def __init__(
        self,
        jwks_url: str,
        algorithms: list[str],
        issuer: str,
        *,
        jwk_client: PyJWKClient | None = None,
    ) -> None:
        self._client = jwk_client or PyJWKClient(
            jwks_url, cache_keys=True, lifespan=300
        )
        self._algorithms = list(algorithms)
        self._issuer = issuer

    def verify(
        self, token: str, *, required_claims: list[str], audience: str | None
    ) -> dict[str, Any]:
        # PyJWKClient reads the unverified header for `kid`; an unknown kid
        # triggers a JWKS refetch (rotation). Missing/unknown kid -> error.
        signing_key = self._client.get_signing_key_from_jwt(token)
        return _decode(
            token,
            signing_key.key,
            algorithms=self._algorithms,
            issuer=self._issuer,
            required_claims=required_claims,
            audience=audience,
        )


def build_verifier(settings: HttpSettings) -> TokenVerifier:
    """Construct the verifier for the configured identity mode."""
    if settings.jwt_jwks_url:
        return JwksVerifier(
            settings.jwt_jwks_url, settings.jwt_algorithms, settings.jwt_issuer
        )
    assert settings.jwt_secret is not None  # enforced by settings validator
    return HmacVerifier(
        settings.jwt_secret, settings.jwt_algorithms, settings.jwt_issuer
    )


__all__ = ["HmacVerifier", "JwksVerifier", "TokenVerifier", "build_verifier"]
