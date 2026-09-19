"""Read models: balances (eventual, per account) and committed posting sets (ADR-0015)."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from cloudscale.domain.errors import DomainError
from cloudscale.entrypoints.http.auth import Principal, authorize_account
from cloudscale.entrypoints.http.context import (
    API_VERSION,
    AUTH_RESPONSES,
    RouteContext,
)
from cloudscale.entrypoints.http.models import (
    BalanceResponse,
    TransferLegResponse,
    TransferResponse,
)


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

    @router.get(
        f"/{API_VERSION}/transfers/{{transfer_id}}",
        response_model=TransferResponse,
        responses={
            404: {
                "description": (
                    "No such posting set projected yet; poll after the write, or the "
                    "caller may read none of its accounts."
                )
            },
            **AUTH_RESPONSES,
        },
    )
    def get_transfer(
        transfer_id: UUID,
        principal: Principal | None = Depends(ctx.query_auth),
    ) -> TransferResponse:
        """A committed posting set and whether a reversal names it (ADR-0015).

        Visible to a caller who may read at least one leg's account; amounts on
        legs whose account the caller may not read are redacted (the ADR-0011
        rule). A set the caller may read nothing of is a 404, not a 403, so the
        existence of other people's payments is not disclosed.
        """
        view = ctx.query_service.get_transfer(transfer_id)
        if view is None:
            raise HTTPException(status_code=404, detail="transfer not found")
        readable = [
            principal is None or ctx.may_read(principal, leg.account_id)
            for leg in view.legs
        ]
        if not any(readable):
            raise HTTPException(status_code=404, detail="transfer not found")
        return TransferResponse(
            transfer_id=view.transfer_id,
            kind=view.kind,
            legs=[
                TransferLegResponse(
                    account_id=leg.account_id,
                    amount=leg.amount if may else None,
                    direction=leg.direction,  # type: ignore[arg-type]
                )
                for leg, may in zip(view.legs, readable, strict=True)
            ],
            reverted_by=view.reverted_by,
            reverts=view.reverts,
        )

    return router


__all__ = ["build_router"]
