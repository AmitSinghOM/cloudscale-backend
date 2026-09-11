"""JWT bearer authentication and account authorization for the HTTP tier.

Authentication is minimal by design (HS256 shared secret) but strict:
expired, unsigned, wrongly signed, wrong-issuer, wrong-audience, and
subject-less tokens are all rejected with a single fixed 401 message (no
token-parser fingerprinting).

Authorization is claims-based plus ownership-based. A token grants access to
the accounts it names, and the system's own ownership registry grants access
to accounts the subject registered:

- ``accounts``: list of account ids the subject may command and read;
- ``scope``: space-separated scopes; ``accounts:admin`` grants all accounts;
- registry: ``owner_of(account_id) == subject`` (see ``POST /v1/accounts``).

Anything else is 403.
"""

from __future__ import annotations

from dataclasses import dataclass

import jwt
from fastapi import Depends, HTTPException, Request

from cloudscale.application.ports import AccountRegistry
from cloudscale.entrypoints.http.settings import HttpSettings
from cloudscale.entrypoints.http.verifiers import TokenVerifier

ADMIN_SCOPE = "accounts:admin"
_INVALID_TOKEN = "invalid or expired bearer token"


@dataclass(frozen=True, slots=True)
class Principal:
    """Verified caller identity and the accounts it may act on."""

    issuer: str
    subject: str
    account_ids: frozenset[str]
    scopes: frozenset[str]

    def may_access(self, account_id: str) -> bool:
        return ADMIN_SCOPE in self.scopes or account_id in self.account_ids


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status_code=401,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


def _get_settings(request: Request) -> HttpSettings:
    settings = getattr(request.app.state, "settings", None)
    if not isinstance(settings, HttpSettings):
        raise RuntimeError("app.state.settings must be an HttpSettings instance")
    return settings


def _string_list(value: object, claim: str) -> frozenset[str]:
    if value is None:
        return frozenset()
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        raise _unauthorized(f"token claim {claim!r} must be a list of strings")
    return frozenset(value)


def _get_verifier(request: Request) -> TokenVerifier:
    verifier = getattr(request.app.state, "token_verifier", None)
    if verifier is None:
        raise RuntimeError("app.state.token_verifier must be set by create_app")
    return verifier


def authenticate(
    request: Request,
    settings: HttpSettings = Depends(_get_settings),
    verifier: TokenVerifier = Depends(_get_verifier),
) -> Principal:
    """Verify the Bearer token and return the caller's principal.

    Beyond signature/issuer/expiry (delegated to the configured verifier):
    ``iat`` is required and the token's lifetime (``exp - iat``) must not
    exceed ``jwt_max_lifetime_seconds``; admin-scoped tokens must carry a
    ``jti`` and are refused if it is on the revocation list.
    """
    authorization = request.headers.get("Authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise _unauthorized("missing bearer token")
    required = ["sub", "exp", "iss", "iat"]
    if settings.jwt_audience is not None:
        required.append("aud")
    try:
        claims = verifier.verify(
            token.strip(), required_claims=required, audience=settings.jwt_audience
        )
    except jwt.InvalidTokenError as error:
        # One fixed message: never echo the parser's exception class.
        raise _unauthorized(_INVALID_TOKEN) from error

    lifetime = _int_claim(claims, "exp") - _int_claim(claims, "iat")
    if lifetime > settings.jwt_max_lifetime_seconds:
        raise _unauthorized(_INVALID_TOKEN)

    subject = claims.get("sub")
    if not isinstance(subject, str) or not subject:
        raise _unauthorized(_INVALID_TOKEN)
    scope_value = claims.get("scope", "")
    if not isinstance(scope_value, str):
        raise _unauthorized("token claim 'scope' must be a string")
    scopes = frozenset(scope_value.split())

    if ADMIN_SCOPE in scopes:
        jti = claims.get("jti")
        if not isinstance(jti, str) or not jti:
            raise _unauthorized("admin-scoped tokens must carry a jti")
        if jti in settings.jwt_revoked_jtis:
            raise _unauthorized(_INVALID_TOKEN)

    return Principal(
        issuer=str(claims["iss"]),
        subject=subject,
        account_ids=_string_list(claims.get("accounts"), "accounts"),
        scopes=scopes,
    )


def _int_claim(claims: dict, name: str) -> int:
    value = claims.get(name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _unauthorized(_INVALID_TOKEN)
    return int(value)


def authorize_account(
    principal: Principal,
    account_id: str,
    registry: AccountRegistry | None = None,
) -> None:
    """Raise 403 unless ``principal`` may act on ``account_id``.

    Access is granted by the admin scope, by the token naming the account, or
    by the registry recording the caller as the account's owner. Everything
    else — including accounts nobody has registered — is denied.
    """
    if principal.may_access(account_id):
        return
    if registry is not None and registry.owner_of(account_id) == principal.subject:
        return
    raise HTTPException(
        status_code=403, detail="principal is not authorized for this account"
    )


__all__ = ["ADMIN_SCOPE", "Principal", "authenticate", "authorize_account"]
