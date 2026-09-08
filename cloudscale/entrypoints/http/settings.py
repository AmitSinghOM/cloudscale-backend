"""HTTP tier configuration, environment-driven and fail-closed.

``jwt_secret`` has no default on purpose: the app refuses to construct
without one, so an unauthenticated deployment cannot happen by omission.
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
    #: Queries also require auth unless explicitly relaxed; writes ALWAYS do.
    query_auth_required: bool = True


__all__ = ["HttpSettings"]
