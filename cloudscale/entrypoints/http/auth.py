"""JWT bearer authentication for the HTTP tier.

Minimal by design (HS256 shared secret) but strict: expired, unsigned,
wrongly signed, wrong-issuer, and subject-less tokens are all rejected with
401. The verified principal carries the ``issuer``/``subject`` pair the
command normalization layer hashes into idempotency identity.
"""

from __future__ import annotations

from dataclasses import dataclass

import jwt
from fastapi import Depends, HTTPException, Request

from cloudscale.entrypoints.http.settings import HttpSettings


@dataclass(frozen=True, slots=True)
class Principal:
    """Verified caller identity."""

    issuer: str
    subject: str


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


def authenticate(
    request: Request, settings: HttpSettings = Depends(_get_settings)
) -> Principal:
    """Verify the Bearer token and return the caller's principal."""
    authorization = request.headers.get("Authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise _unauthorized("missing bearer token")
    try:
        claims = jwt.decode(
            token.strip(),
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm],
            issuer=settings.jwt_issuer,
            options={"require": ["sub", "exp", "iss"]},
        )
    except jwt.InvalidTokenError as error:
        raise _unauthorized(f"invalid token: {type(error).__name__}") from error
    subject = claims.get("sub")
    if not isinstance(subject, str) or not subject:
        raise _unauthorized("token subject must be a non-empty string")
    return Principal(issuer=str(claims["iss"]), subject=subject)


__all__ = ["Principal", "authenticate"]
