"""HTTP tier configuration, environment-driven and fail-closed.

Identity is configured in exactly one of two modes, and the app refuses to
construct otherwise — an unauthenticated or ambiguously-authenticated
deployment cannot happen by omission:

- **shared secret** (``jwt_secret``): HS256, for single-tenant / internal use;
- **OIDC / JWKS** (``jwt_jwks_url``): RS256 / ES256 keys fetched from the
  issuer's JWKS endpoint with ``kid``-based rotation.

Abuse controls are ON by default; disabling any of them is an explicit,
logged deployment decision (the load harness does this and records it).
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_HMAC_ALGORITHMS = frozenset({"HS256", "HS384", "HS512"})
_ASYMMETRIC_ALGORITHMS = frozenset({"RS256", "RS384", "RS512", "ES256", "ES384"})


class HttpSettings(BaseSettings):
    """Environment-driven settings (prefix ``CLOUDSCALE_``)."""

    model_config = SettingsConfigDict(env_prefix="CLOUDSCALE_")

    # -- identity: exactly one of the two modes ---------------------------------
    #: RFC 7518 §3.2: HS256 keys must be at least 32 bytes.
    jwt_secret: str | None = Field(default=None, min_length=32)
    #: OIDC JWKS endpoint, e.g. https://issuer.example/.well-known/jwks.json
    jwt_jwks_url: str | None = None
    #: Accepted algorithms; defaults follow the mode (HS256 | RS256+ES256).
    jwt_algorithms: list[str] = Field(default_factory=list)
    jwt_issuer: str = "cloudscale"
    #: Optional audience claim check; when set, tokens must carry it.
    jwt_audience: str | None = None
    #: Reject tokens whose exp - iat exceeds this (requires ``iat``).
    jwt_max_lifetime_seconds: int = Field(default=3600, ge=1)
    #: Revoked token ids; admin-scoped tokens MUST carry ``jti`` and are
    #: refused if listed. Env: comma-separated.
    jwt_revoked_jtis: list[str] = Field(default_factory=list)

    #: Queries also require auth unless explicitly relaxed; writes ALWAYS do.
    query_auth_required: bool = True

    #: Per-subject token bucket, requests per minute. 0 disables (bench only).
    rate_limit_per_minute: int = Field(default=600, ge=0)
    #: ``memory`` bounds one replica; ``postgres`` shares one budget across
    #: every replica (requires the postgres storage tier).
    rate_limit_backend: Literal["memory", "postgres"] = "memory"
    #: Pre-authentication per-client-address budget, requests per minute.
    #: Bounds the cost of unauthenticated floods (token verification is not
    #: free). Per replica, in memory. 0 disables (bench only).
    client_rate_limit_per_minute: int = Field(default=1_200, ge=0)
    #: Trust ``X-Forwarded-For`` for the client address. Enable ONLY behind
    #: a proxy you control that overwrites the header; otherwise clients can
    #: spoof their way out of the client limiter.
    trust_proxy_headers: bool = False
    #: Maximum accepted request body, bytes (commands are ~150 bytes).
    max_body_bytes: int = Field(default=16_384, ge=1)
    #: CORS allowlist. Empty (default) = no cross-origin browser access.
    cors_origins: list[str] = Field(default_factory=list)

    @property
    def identity_mode(self) -> str:
        return "jwks" if self.jwt_jwks_url else "hmac"

    @model_validator(mode="after")
    def _exactly_one_identity_mode(self) -> HttpSettings:
        if bool(self.jwt_secret) == bool(self.jwt_jwks_url):
            raise ValueError(
                "configure exactly one of CLOUDSCALE_JWT_SECRET (shared secret) "
                "or CLOUDSCALE_JWT_JWKS_URL (OIDC/JWKS)"
            )
        if not self.jwt_algorithms:
            self.jwt_algorithms = ["RS256", "ES256"] if self.jwt_jwks_url else ["HS256"]
        allowed = _ASYMMETRIC_ALGORITHMS if self.jwt_jwks_url else _HMAC_ALGORITHMS
        bad = sorted(set(self.jwt_algorithms) - allowed)
        if bad:
            # Refuse algorithm confusion at configuration time: HMAC algs with
            # a public-key source (or vice versa) can never be correct.
            raise ValueError(
                f"algorithms {bad} are not valid for identity mode "
                f"{self.identity_mode!r}; allowed: {sorted(allowed)}"
            )
        return self


__all__ = ["HttpSettings"]
