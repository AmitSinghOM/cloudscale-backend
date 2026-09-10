"""HTTP tier configuration, environment-driven and fail-closed.

``jwt_secret`` has no default on purpose: the app refuses to construct
without one, so an unauthenticated deployment cannot happen by omission.
Abuse controls are ON by default; disabling any of them is an explicit,
logged deployment decision (the load harness does this and records it).
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class HttpSettings(BaseSettings):
    """Environment-driven settings (prefix ``CLOUDSCALE_``)."""

    model_config = SettingsConfigDict(env_prefix="CLOUDSCALE_")

    #: RFC 7518 §3.2: HS256 keys must be at least 32 bytes.
    jwt_secret: str = Field(min_length=32)
    jwt_algorithm: str = "HS256"
    jwt_issuer: str = "cloudscale"
    #: Optional audience claim check; when set, tokens must carry it.
    jwt_audience: str | None = None
    #: Queries also require auth unless explicitly relaxed; writes ALWAYS do.
    query_auth_required: bool = True

    #: Per-subject token bucket, requests per minute. 0 disables (bench only).
    rate_limit_per_minute: int = Field(default=600, ge=0)
    #: Maximum accepted request body, bytes (commands are ~150 bytes).
    max_body_bytes: int = Field(default=16_384, ge=1)
    #: CORS allowlist. Empty (default) = no cross-origin browser access.
    cors_origins: list[str] = Field(default_factory=list)


__all__ = ["HttpSettings"]
