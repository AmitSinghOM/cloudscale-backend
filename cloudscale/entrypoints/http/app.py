"""FastAPI application factory for the CloudScale HTTP tier.

Command and query paths both run through the typed application layer. The
command endpoint requires authentication unconditionally; the caller supplies
``command_id`` as the idempotency key, and the HTTP status comes straight
from the persisted, transport-neutral ``CommandResult``.
"""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from cloudscale.application.command_service import CommandService
from cloudscale.application.query_service import QueryService
from cloudscale.domain.commands import Deposit, Withdraw
from cloudscale.domain.errors import DomainError
from cloudscale.domain.results import CommandResult
from cloudscale.entrypoints.http.auth import Principal, authenticate
from cloudscale.entrypoints.http.settings import HttpSettings

API_VERSION = "v1"


class CommandRequest(BaseModel):
    """One account command; ``command_id`` is the caller-owned idempotency key."""

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
) -> FastAPI:
    """Build the HTTP app over explicit, injected collaborators."""
    app = FastAPI(title="cloudscale-backend", version=API_VERSION)
    app.state.settings = settings

    if settings.query_auth_required:
        query_auth = Depends(authenticate)
    else:  # explicitly relaxed reads; writes always authenticate
        query_auth = Depends(lambda: None)

    @app.get(f"/{API_VERSION}/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(status="ok", storage=storage_metadata)

    @app.post(f"/{API_VERSION}/accounts/{{account_id}}/commands")
    def post_command(
        account_id: str,
        request: CommandRequest,
        principal: Principal = Depends(authenticate),  # writes ALWAYS authenticate
    ) -> JSONResponse:
        try:
            command = (
                Deposit(account_id, request.amount, request.expected_version)
                if request.type == "deposit"
                else Withdraw(account_id, request.amount, request.expected_version)
            )
        except DomainError as error:
            raise HTTPException(status_code=400, detail=error.code) from error
        result = command_service.execute(
            command,
            command_id=request.command_id,
            issuer=principal.issuer,
            subject=principal.subject,
        )
        return _command_response(result)

    @app.get(
        f"/{API_VERSION}/accounts/{{account_id}}/balance",
        response_model=BalanceResponse,
    )
    def get_balance(
        account_id: str,
        _principal: Principal | None = query_auth,
    ) -> BalanceResponse:
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


__all__ = ["API_VERSION", "BalanceResponse", "HealthResponse", "create_app"]
