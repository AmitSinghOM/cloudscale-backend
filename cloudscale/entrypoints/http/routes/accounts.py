"""Deprecated self-registration endpoint; ownership reads remain available."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from cloudscale.entrypoints.http.auth import Principal
from cloudscale.entrypoints.http.context import (
    API_VERSION,
    AUTH_RESPONSES,
    RouteContext,
)
from cloudscale.entrypoints.http.models import RegisterAccountRequest


def build_router(ctx: RouteContext) -> APIRouter:
    router = APIRouter()

    @router.post(
        f"/{API_VERSION}/accounts",
        status_code=403,
        summary="Self-registration disabled (ADR-0021)",
        responses=AUTH_RESPONSES,
    )
    def register_account(
        _request: RegisterAccountRequest,
        _principal: Principal = Depends(ctx.authenticated),
    ) -> None:
        raise HTTPException(
            status_code=403, detail="principal is not authorized for this account"
        )

    return router


__all__ = ["build_router"]
