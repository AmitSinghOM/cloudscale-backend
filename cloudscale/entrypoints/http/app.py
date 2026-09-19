"""FastAPI application factory for the CloudScale HTTP tier.

Command and query paths both run through the typed application layer.
Every request is authenticated (queries relaxable by explicit setting);
every account access is authorized against the token's claims; every
command decision is audit-logged and counted. Abuse controls (per-subject
rate limit, body-size cap, CORS allowlist) are on by default.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from cloudscale.application.command_service import CommandService
from cloudscale.application.ports import AccountRegistry
from cloudscale.application.query_service import QueryService
from cloudscale.entrypoints.http.auth import Principal, authenticate
from cloudscale.entrypoints.http.context import (
    API_VERSION,
    CommandExecutor,
    RouteContext,
)
from cloudscale.entrypoints.http.limits import (
    BodySizeLimitMiddleware,
    ClientRateLimitMiddleware,
    RateLimiter,
    RateLimiterLike,
)
from cloudscale.entrypoints.http.models import (
    BalanceResponse,
    Closeable,
    CommandRequest,
    HealthResponse,
    ReadinessProbe,
)
from cloudscale.entrypoints.http.observability import (
    Metrics,
    RequestLogMiddleware,
)
from cloudscale.entrypoints.http.routes import accounts, commands, holds, ops, queries
from cloudscale.entrypoints.http.settings import HttpSettings
from cloudscale.entrypoints.http.verifiers import TokenVerifier, build_verifier
from cloudscale.resilience import CircuitBreaker, RetryPolicy

if TYPE_CHECKING:
    from opentelemetry.trace import TracerProvider

#: Appears in the OpenAPI document as the bearer security scheme so generated
#: clients send Authorization. It does NOT authenticate: ``auto_error=False``
#: and the value is ignored; ``auth.authenticate`` remains the single verifier
#: (fixed 401 text, lifetime cap, jti revocation).
_BEARER = HTTPBearer(auto_error=False, scheme_name="bearerAuth")

_LOGGER = logging.getLogger("cloudscale.http")


def create_app(
    settings: HttpSettings,
    *,
    command_service: CommandService,
    query_service: QueryService,
    storage_metadata: dict[str, object],
    transient_errors: tuple[type[BaseException], ...] = (sqlite3.OperationalError,),
    retry_policy: RetryPolicy | None = None,
    breaker: CircuitBreaker | None = None,
    tracer_provider: TracerProvider | None = None,
    rate_limiter: RateLimiterLike | None = None,
    account_registry: AccountRegistry | None = None,
    token_verifier: TokenVerifier | None = None,
    readiness_probe: ReadinessProbe | None = None,
    closeables: Sequence[Closeable] = (),
) -> FastAPI:
    """Build the HTTP app over explicit, injected collaborators.

    The command path runs under Phase 2 resilience: each attempt is guarded
    by a circuit breaker that counts only ``transient_errors`` as failures
    (a deterministic rejection proves the downstream processed the call), and
    transient failures are retried under ``retry_policy``. An open circuit or
    an exhausted retry budget maps to 503 with ``Retry-After`` — the caller
    may safely retry with the SAME command_id thanks to the idempotent unit
    of work. ``closeables`` are closed on application shutdown.
    """
    command_retry = retry_policy or RetryPolicy(max_attempts=3)
    command_breaker = breaker or CircuitBreaker(counted_errors=transient_errors)
    limiter = rate_limiter or RateLimiter(settings.rate_limit_per_minute)
    metrics = Metrics()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            for resource in closeables:
                resource.close()

    app = FastAPI(title="cloudscale-backend", version=API_VERSION, lifespan=lifespan)
    app.state.settings = settings
    app.state.metrics = metrics
    app.state.token_verifier = token_verifier or build_verifier(settings)

    # Middleware order (outermost first): access log -> client limit -> body
    # cap -> CORS. The client limiter sits inside the access log so pre-auth
    # 429s are still logged and counted; it runs before any body is read or
    # any token is verified.
    app.add_middleware(RequestLogMiddleware, metrics=metrics)
    if settings.client_rate_limit_per_minute > 0:
        app.add_middleware(
            ClientRateLimitMiddleware,
            limiter=RateLimiter(settings.client_rate_limit_per_minute),
            trust_proxy=settings.trust_proxy_headers,
            exempt_paths=frozenset(
                {f"/{API_VERSION}/health", f"/{API_VERSION}/ready", "/metrics"}
            ),
        )
    app.add_middleware(BodySizeLimitMiddleware, max_bytes=settings.max_body_bytes)
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_methods=["GET", "POST"],
            allow_headers=["Authorization", "Content-Type"],
        )

    if tracer_provider is not None:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app, tracer_provider=tracer_provider)

    def _rate_limited(request: Request, principal: Principal) -> None:
        request.state.subject = principal.subject
        allowed, retry_after = limiter.try_acquire(principal.subject)
        if not allowed:
            raise HTTPException(
                status_code=429,
                detail="rate limit exceeded",
                headers={"Retry-After": str(max(1, int(retry_after + 0.999)))},
            )

    def authenticated(
        request: Request,
        principal: Principal = Depends(authenticate),
        _schema_only: HTTPAuthorizationCredentials | None = Depends(_BEARER),
    ) -> Principal:
        _rate_limited(request, principal)
        return principal

    if settings.query_auth_required:
        query_auth: Callable[..., Principal | None] = authenticated
    else:  # explicitly relaxed reads; writes always authenticate
        query_auth = lambda: None  # noqa: E731 -- a dependency, not a helper

    ctx = RouteContext(
        settings=settings,
        executor=CommandExecutor(
            command_service,
            retry_policy=command_retry,
            breaker=command_breaker,
            transient_errors=transient_errors,
            metrics=metrics,
        ),
        query_service=query_service,
        account_registry=account_registry,
        authenticated=authenticated,
        query_auth=query_auth,
        metrics=metrics,
    )
    # Order matters only for the committed OpenAPI document (docs/openapi.json
    # lists paths in registration order); keep it stable.
    app.include_router(
        ops.build_router(
            ctx, storage_metadata=storage_metadata, readiness_probe=readiness_probe
        )
    )
    app.include_router(accounts.build_router(ctx))
    app.include_router(commands.build_router(ctx))
    app.include_router(holds.build_router(ctx))
    app.include_router(queries.build_router(ctx))
    return app


__all__ = [
    "API_VERSION",
    "BalanceResponse",
    "CommandRequest",
    "HealthResponse",
    "create_app",
]
