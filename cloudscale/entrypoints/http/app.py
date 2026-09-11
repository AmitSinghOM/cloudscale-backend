"""FastAPI application factory for the CloudScale HTTP tier.

Command and query paths both run through the typed application layer.
Every request is authenticated (queries relaxable by explicit setting);
every account access is authorized against the token's claims; every
command decision is audit-logged and counted. Abuse controls (per-subject
rate limit, body-size cap, CORS allowlist) are on by default.
"""

from __future__ import annotations

import sqlite3
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from functools import partial
from typing import TYPE_CHECKING, Literal, Protocol
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from cloudscale.application.command_service import CommandService
from cloudscale.application.ports import AccountRegistry, RegistrationOutcome
from cloudscale.application.query_service import QueryService
from cloudscale.domain.commands import Deposit, Withdraw
from cloudscale.domain.errors import DomainError
from cloudscale.domain.results import CommandResult
from cloudscale.entrypoints.http.auth import (
    Principal,
    authenticate,
    authorize_account,
)
from cloudscale.entrypoints.http.limits import (
    BodySizeLimitMiddleware,
    RateLimiter,
    RateLimiterLike,
)
from cloudscale.entrypoints.http.observability import (
    Metrics,
    RequestLogMiddleware,
    audit_command,
    audit_registration,
)
from cloudscale.entrypoints.http.settings import HttpSettings
from cloudscale.entrypoints.http.verifiers import TokenVerifier, build_verifier
from cloudscale.resilience import (
    CircuitBreaker,
    CircuitOpenError,
    RetryPolicy,
    call_with_retry,
)

if TYPE_CHECKING:
    from opentelemetry.trace import TracerProvider

API_VERSION = "v1"


class Closeable(Protocol):
    def close(self) -> None: ...


class RegisterAccountRequest(BaseModel):
    """Claim ownership of an account id for the authenticated caller."""

    model_config = ConfigDict(extra="forbid")

    account_id: str


class CommandRequest(BaseModel):
    """One account command; ``command_id`` is the caller-owned idempotency key."""

    # Unknown fields are a client bug (e.g. a misspelled expected_version);
    # surface them as 422 instead of silently ignoring them.
    model_config = ConfigDict(extra="forbid")

    command_id: UUID
    type: Literal["deposit", "withdraw"]
    amount: int
    expected_version: int


class CommandResponse(BaseModel):
    command_id: UUID
    outcome: str
    account_id: str
    expected_version: int
    current_version: int
    committed_version: int | None
    event_id: UUID | None
    correlation_id: UUID
    error_code: str | None


def _command_response(result: CommandResult) -> JSONResponse:
    body = CommandResponse(
        command_id=result.command_id,
        outcome=result.outcome.value,
        account_id=result.account_id,
        expected_version=result.expected_version,
        current_version=result.current_version,
        committed_version=result.committed_version,
        event_id=result.event_id,
        correlation_id=result.correlation_id,
        error_code=result.error_code,
    )
    return JSONResponse(
        status_code=result.http_status, content=body.model_dump(mode="json")
    )


class BalanceResponse(BaseModel):
    account_id: str
    balance: int
    version: int
    #: Read models are projections of the log; reads can trail writes.
    consistency: str = "eventual"


class HealthResponse(BaseModel):
    status: str
    storage: dict[str, object]


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

    # Middleware order (outermost first): access log -> body cap -> CORS.
    app.add_middleware(RequestLogMiddleware, metrics=metrics)
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
        request: Request, principal: Principal = Depends(authenticate)
    ) -> Principal:
        _rate_limited(request, principal)
        return principal

    if settings.query_auth_required:
        query_auth = Depends(authenticated)
    else:  # explicitly relaxed reads; writes always authenticate
        query_auth = Depends(lambda: None)

    @app.get(f"/{API_VERSION}/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(status="ok", storage=storage_metadata)

    @app.get("/metrics", include_in_schema=False)
    def prometheus_metrics() -> object:
        return metrics.render()

    @app.post(f"/{API_VERSION}/accounts")
    def register_account(
        request: RegisterAccountRequest,
        principal: Principal = Depends(authenticated),
    ) -> JSONResponse:
        """Bind an account to the caller. Idempotent for the same caller."""
        if account_registry is None:
            raise HTTPException(
                status_code=501, detail="account registration is not enabled"
            )
        try:
            outcome = account_registry.register(request.account_id, principal.subject)
        except DomainError as error:
            raise HTTPException(status_code=400, detail=error.code) from error
        audit_registration(
            subject=principal.subject,
            issuer=principal.issuer,
            account_id=request.account_id,
            outcome=outcome.value,
        )
        status = {
            RegistrationOutcome.CREATED: 201,
            RegistrationOutcome.ALREADY_OWNED_BY_CALLER: 200,
            RegistrationOutcome.TAKEN: 409,
        }[outcome]
        return JSONResponse(
            status_code=status,
            content={
                "account_id": request.account_id,
                "outcome": outcome.value,
                "owner": principal.subject if status != 409 else None,
            },
        )

    @app.post(f"/{API_VERSION}/accounts/{{account_id}}/commands")
    def post_command(
        account_id: str,
        request: CommandRequest,
        principal: Principal = Depends(authenticated),  # writes ALWAYS authenticate
    ) -> JSONResponse:
        authorize_account(principal, account_id, account_registry)
        try:
            command = (
                Deposit(account_id, request.amount, request.expected_version)
                if request.type == "deposit"
                else Withdraw(account_id, request.amount, request.expected_version)
            )
        except DomainError as error:
            raise HTTPException(status_code=400, detail=error.code) from error

        def _attempt() -> CommandResult:
            return command_breaker.call(
                partial(
                    command_service.execute,
                    command,
                    command_id=request.command_id,
                    issuer=principal.issuer,
                    subject=principal.subject,
                )
            )

        try:
            result = call_with_retry(
                _attempt, command_retry, retryable_errors=transient_errors
            )
        except CircuitOpenError as error:
            raise HTTPException(
                status_code=503,
                detail="command path unavailable (circuit open)",
                headers={"Retry-After": str(max(1, int(error.retry_after_seconds)))},
            ) from error
        except transient_errors as error:
            raise HTTPException(
                status_code=503,
                detail="command path unavailable (transient storage failure)",
                headers={"Retry-After": "1"},
            ) from error

        audit_command(
            subject=principal.subject,
            issuer=principal.issuer,
            account_id=account_id,
            command_type=request.type,
            command_id=request.command_id,
            result=result,
        )
        metrics.observe_command(result)
        return _command_response(result)

    @app.get(
        f"/{API_VERSION}/accounts/{{account_id}}/balance",
        response_model=BalanceResponse,
    )
    def get_balance(
        account_id: str,
        principal: Principal | None = query_auth,
    ) -> BalanceResponse:
        if principal is not None:
            authorize_account(principal, account_id, account_registry)
        try:
            view = query_service.get_balance(account_id)
        except DomainError as error:
            raise HTTPException(status_code=400, detail=error.code) from error
        if view.version == 0:
            raise HTTPException(status_code=404, detail="account not found")
        return BalanceResponse(
            account_id=view.account_id,
            balance=view.balance,
            version=view.version,
        )

    return app


__all__ = [
    "API_VERSION",
    "BalanceResponse",
    "CommandRequest",
    "HealthResponse",
    "create_app",
]
