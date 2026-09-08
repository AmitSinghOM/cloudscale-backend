"""FastAPI application factory for the CloudScale HTTP tier.

Phase 4 increment one: health with storage-tier disclosure and the query
endpoint through the typed application layer. The command endpoint lands
with the concrete ``CommandUnitOfWork`` adapter (see ROADMAP) — no write
path is exposed until it can run through the idempotent unit of work.
"""

from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from cloudscale.application.query_service import QueryService
from cloudscale.domain.errors import DomainError
from cloudscale.entrypoints.http.auth import Principal, authenticate
from cloudscale.entrypoints.http.settings import HttpSettings

API_VERSION = "v1"


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
