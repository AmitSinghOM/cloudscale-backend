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
from functools import partial
from typing import TYPE_CHECKING, Literal, Protocol
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from pydantic import BaseModel, ConfigDict

from cloudscale.application.command_service import CommandService
from cloudscale.application.ports import AccountRegistry, RegistrationOutcome
from cloudscale.application.query_service import QueryService
from cloudscale.domain.commands import (
    AccountCommand,
    Deposit,
    Leg,
    Post,
    Transfer,
    Withdraw,
)
from cloudscale.domain.errors import DomainError
from cloudscale.domain.results import CommandResult
from cloudscale.domain.upcasting import UnknownSchemaVersionError
from cloudscale.entrypoints.http.auth import (
    Principal,
    authenticate,
    authorize_account,
)
from cloudscale.entrypoints.http.limits import (
    BodySizeLimitMiddleware,
    ClientRateLimitMiddleware,
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

#: Appears in the OpenAPI document as the bearer security scheme so generated
#: clients send Authorization. It does NOT authenticate: ``auto_error=False``
#: and the value is ignored; ``auth.authenticate`` remains the single verifier
#: (fixed 401 text, lifetime cap, jti revocation).
_BEARER = HTTPBearer(auto_error=False, scheme_name="bearerAuth")

#: Documented non-2xx responses shared by the authenticated account routes.
#: The bodies are described in docs/API_ERRORS.md; declaring them here makes
#: the committed contract (docs/openapi.json) honest for client generators.
_AUTH_RESPONSES: dict[int | str, dict[str, object]] = {
    401: {"description": "Missing, invalid or expired bearer token (fixed message)."},
    403: {"description": "Token valid but not authorized for this account."},
    429: {"description": "Rate limit exceeded; honour Retry-After."},
    503: {
        "description": "Command path unavailable; retry with the SAME command_id after Retry-After."
    },
}

API_VERSION = "v1"
_LOGGER = logging.getLogger("cloudscale.http")


class Closeable(Protocol):
    def close(self) -> None: ...


class ReadinessProbe(Protocol):
    """Answers "can this replica serve traffic right now?".

    Must touch the real dependencies (a storage round-trip, the schema
    revision) and raise on any failure; the caller maps exceptions to 503.
    Returns a small dict of what was checked, for the response body.
    """

    def check(self) -> dict[str, object]: ...


class ReadyResponse(BaseModel):
    status: Literal["ready", "not_ready"]
    checks: dict[str, object]


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


class TransferRequest(BaseModel):
    """Move ``amount`` from the path account to ``target_account_id`` (ADR-0011).

    ``expected_version`` is the source stream's version; the target has no
    client-supplied guard.
    """

    model_config = ConfigDict(extra="forbid")

    command_id: UUID
    target_account_id: str
    amount: int
    expected_version: int


class LegRequest(BaseModel):
    """One leg of a posting set (ADR-0013)."""

    model_config = ConfigDict(extra="forbid")

    account_id: str
    amount: int
    direction: Literal["debit", "credit"]


class PostingsRequest(BaseModel):
    """Commit 2..MAX_LEGS balanced legs atomically; the path account is the anchor.

    The anchor must be one of the debited legs and is the only stream whose
    ``expected_version`` the caller supplies. Validation (balance, duplicate
    accounts, leg count, anchor debited) is the domain's; failures are 400s
    with the domain code.
    """

    model_config = ConfigDict(extra="forbid")

    command_id: UUID
    postings: list[LegRequest]
    expected_version: int


class PostingResponse(BaseModel):
    """One stream an accepted command wrote.

    ``committed_version`` is present only for accounts the caller may read: a
    stream version counts that account's activity, and a transfer's target
    is not necessarily the caller's account.
    """

    account_id: str
    event_id: UUID
    committed_version: int | None


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
    postings: list[PostingResponse]


def _unavailable(reason: str, retry_after_seconds: int) -> HTTPException:
    """503 for the command path with the cause a client may log and a Retry-After."""
    return HTTPException(
        status_code=503,
        detail=f"command path unavailable ({reason})",
        headers={"Retry-After": str(retry_after_seconds)},
    )


def _command_response(
    result: CommandResult, *, may_read: Callable[[str], bool] = lambda _: True
) -> JSONResponse:
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
        postings=[
            PostingResponse(
                account_id=posting.account_id,
                event_id=posting.event_id,
                committed_version=(
                    posting.committed_version if may_read(posting.account_id) else None
                ),
            )
            for posting in result.postings
        ],
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
        query_auth = Depends(authenticated)
    else:  # explicitly relaxed reads; writes always authenticate
        query_auth = Depends(lambda: None)

    @app.get(f"/{API_VERSION}/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        # Liveness only: the process is up. Never touches storage, so it
        # cannot be used to decide whether to route traffic here.
        return HealthResponse(status="ok", storage=storage_metadata)

    @app.get(
        f"/{API_VERSION}/ready",
        response_model=ReadyResponse,
        responses={503: {"model": ReadyResponse}},
    )
    def ready() -> JSONResponse:
        # Readiness: storage round-trip (and schema revision on the PG tier).
        # 503 tells the orchestrator to stop routing to this replica and to
        # fail a rollout of a build that cannot reach or does not match the
        # database. Without a probe wired, readiness == liveness (SQLite dev).
        if readiness_probe is None:
            return JSONResponse(
                ReadyResponse(status="ready", checks={"probe": "none"}).model_dump()
            )
        try:
            checks = readiness_probe.check()
        except Exception as exc:  # noqa: BLE001 — any failure means not ready
            # Unauthenticated endpoint: expose only the exception class. The
            # message can carry host names or DSN fragments; that goes to the
            # log, where operators (not the internet) read it.
            _LOGGER.warning(
                "readiness probe failed",
                extra={"error_type": type(exc).__name__, "error": str(exc)[:500]},
            )
            body = ReadyResponse(
                status="not_ready", checks={"error": type(exc).__name__}
            )
            return JSONResponse(body.model_dump(), status_code=503)
        return JSONResponse(ReadyResponse(status="ready", checks=checks).model_dump())

    @app.get("/metrics", include_in_schema=False)
    def prometheus_metrics() -> object:
        return metrics.render()

    @app.post(
        f"/{API_VERSION}/accounts",
        status_code=201,
        responses={
            200: {"description": "Already registered to this caller (idempotent)."},
            409: {"description": "Registered to a different subject."},
            400: {"description": "invalid_account_id."},
            501: {"description": "Registration not enabled in this deployment."},
            **_AUTH_RESPONSES,
        },
    )
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

    def _execute(
        command: AccountCommand,
        *,
        command_id: UUID,
        principal: Principal,
        command_type: str,
    ) -> CommandResult:
        """Run one command through breaker + retry; map storage faults to 503."""

        def _attempt() -> CommandResult:
            return command_breaker.call(
                partial(
                    command_service.execute,
                    command,
                    command_id=command_id,
                    issuer=principal.issuer,
                    subject=principal.subject,
                )
            )

        try:
            result = call_with_retry(
                _attempt, command_retry, retryable_errors=transient_errors
            )
        except CircuitOpenError as error:
            retry_after = max(1, int(error.retry_after_seconds))
            raise _unavailable("circuit open", retry_after) from error
        except transient_errors as error:
            raise _unavailable("transient storage failure", 1) from error
        except UnknownSchemaVersionError as error:
            # The stream holds an event written by a NEWER build (rolled-back
            # deploy). Not a client error and not transient storage: the fix
            # is redeploying the newer build. 503 tells the client to retry
            # later; the account is frozen, never corrupted (RUNBOOK R1).
            # The request log and metrics only see "503": name the cause here
            # so operators can tell this from a storage outage.
            _LOGGER.error(
                "event schema newer than this build; redeploy the newer build",
                extra={"account_id": command.account_id, "error": str(error)[:500]},
            )
            raise _unavailable("event schema newer than this build", 60) from error

        audit_command(
            subject=principal.subject,
            issuer=principal.issuer,
            account_id=command.account_id,
            command_type=command_type,
            command_id=command_id,
            result=result,
        )
        metrics.observe_command(result)
        return result

    def _may_read(principal: Principal, account_id: str) -> bool:
        if principal.may_access(account_id):
            return True
        return (
            account_registry is not None
            and account_registry.owner_of(account_id) == principal.subject
        )

    @app.post(
        f"/{API_VERSION}/accounts/{{account_id}}/commands",
        status_code=201,
        responses={
            400: {
                "description": "CommandResult with outcome=domain_rejected and error_code."
            },
            409: {
                "description": "CommandResult: version_conflict or command_id_conflict."
            },
            422: {"description": "CommandResult: insufficient_funds."},
            **_AUTH_RESPONSES,
        },
    )
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
        result = _execute(
            command,
            command_id=request.command_id,
            principal=principal,
            command_type=request.type,
        )
        return _command_response(result)

    @app.post(
        f"/{API_VERSION}/accounts/{{account_id}}/transfers",
        status_code=201,
        responses={
            400: {
                "description": (
                    "CommandResult with outcome=domain_rejected and error_code, or "
                    "a request naming the same account twice (same_account)."
                )
            },
            409: {
                "description": (
                    "CommandResult: version_conflict (source stream) or "
                    "command_id_conflict."
                )
            },
            422: {"description": "CommandResult: insufficient_funds on the source."},
            **_AUTH_RESPONSES,
        },
    )
    def post_transfer(
        account_id: str,
        request: TransferRequest,
        principal: Principal = Depends(authenticated),
    ) -> JSONResponse:
        """Move funds from the path account to ``target_account_id`` (ADR-0011).

        Authorization is on the source only: money leaves the caller's
        account; the target may be any account, exactly as a deposit may
        create one. The target posting's ``committed_version`` is redacted
        unless the caller may read that account.
        """
        authorize_account(principal, account_id, account_registry)
        try:
            command = Transfer(
                account_id,
                request.target_account_id,
                request.amount,
                request.expected_version,
            )
        except DomainError as error:
            raise HTTPException(status_code=400, detail=error.code) from error
        result = _execute(
            command,
            command_id=request.command_id,
            principal=principal,
            command_type="transfer",
        )
        return _command_response(
            result, may_read=lambda target: _may_read(principal, target)
        )

    @app.post(
        f"/{API_VERSION}/accounts/{{account_id}}/postings",
        status_code=201,
        responses={
            400: {
                "description": (
                    "CommandResult with outcome=domain_rejected and error_code, or "
                    "an invalid posting set: unbalanced, duplicate_account, "
                    "too_many_legs, anchor_not_debited."
                )
            },
            409: {
                "description": (
                    "CommandResult: version_conflict (anchor stream) or "
                    "command_id_conflict."
                )
            },
            422: {"description": "CommandResult: insufficient_funds on a debited leg."},
            **_AUTH_RESPONSES,
        },
    )
    def post_postings(
        account_id: str,
        request: PostingsRequest,
        principal: Principal = Depends(authenticated),
    ) -> JSONResponse:
        """Commit a balanced N-leg posting set in one transaction (ADR-0013).

        Authorization is on the anchor (the path account), which must be a
        debited leg. Every other leg's ``committed_version`` is redacted unless
        the caller may read that account.
        """
        authorize_account(principal, account_id, account_registry)
        try:
            command = Post(
                account_id,
                tuple(
                    Leg(leg.account_id, leg.amount, leg.direction)
                    for leg in request.postings
                ),
                request.expected_version,
            )
        except DomainError as error:
            raise HTTPException(status_code=400, detail=error.code) from error
        result = _execute(
            command,
            command_id=request.command_id,
            principal=principal,
            command_type="post",
        )
        return _command_response(
            result, may_read=lambda target: _may_read(principal, target)
        )

    @app.get(
        f"/{API_VERSION}/accounts/{{account_id}}/balance",
        response_model=BalanceResponse,
        responses={
            404: {
                "description": "No event projected yet for this account; poll after a write."
            },
            **_AUTH_RESPONSES,
        },
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
