"""Read model: balances (eventual, per account)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from cloudscale.domain.errors import DomainError
from cloudscale.entrypoints.http.auth import Principal, authorize_account
from cloudscale.entrypoints.http.context import (
    API_VERSION,
    AUTH_RESPONSES,
    RouteContext,
)
from cloudscale.entrypoints.http.models import BalanceResponse


def build_router(ctx: RouteContext) -> APIRouter:
    router = APIRouter()

    @router.get(
        f"/{API_VERSION}/accounts/{{account_id}}/balance",
        response_model=BalanceResponse,
        responses={
            404: {
                "description": "No event projected yet for this account; poll after a write."
            },
            **AUTH_RESPONSES,
        },
    )
    def get_balance(
        account_id: str,
        principal: Principal | None = Depends(ctx.query_auth),
    ) -> BalanceResponse:
        if principal is not None:
            authorize_account(principal, account_id, ctx.account_registry)
        try:
            view = ctx.query_service.get_balance(account_id)
        except DomainError as error:
            raise HTTPException(status_code=400, detail=error.code) from error
        if view.version == 0:
            raise HTTPException(status_code=404, detail="account not found")
        return BalanceResponse(
            account_id=view.account_id,
            balance=view.balance,
            held=view.held,
            available=view.available,
            version=view.version,
        )

    return router


__all__ = ["build_router"]
