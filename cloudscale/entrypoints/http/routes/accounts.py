"""Account registration: bind an account id to its owning subject."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from cloudscale.application.ports import RegistrationOutcome
from cloudscale.domain.errors import DomainError
from cloudscale.entrypoints.http.auth import Principal
from cloudscale.entrypoints.http.context import (
    API_VERSION,
    AUTH_RESPONSES,
    RouteContext,
)
from cloudscale.entrypoints.http.models import RegisterAccountRequest
from cloudscale.entrypoints.http.observability import audit_registration


def build_router(ctx: RouteContext) -> APIRouter:
    router = APIRouter()

    @router.post(
        f"/{API_VERSION}/accounts",
        status_code=201,
        responses={
            200: {"description": "Already registered to this caller (idempotent)."},
            409: {"description": "Registered to a different subject."},
            400: {"description": "invalid_account_id."},
            501: {"description": "Registration not enabled in this deployment."},
            **AUTH_RESPONSES,
        },
    )
    def register_account(
        request: RegisterAccountRequest,
        principal: Principal = Depends(ctx.authenticated),
    ) -> JSONResponse:
        """Bind an account to the caller. Idempotent for the same caller."""
        if ctx.account_registry is None:
            raise HTTPException(
                status_code=501, detail="account registration is not enabled"
            )
        try:
            outcome = ctx.account_registry.register(
                request.account_id, principal.subject
            )
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

    return router


__all__ = ["build_router"]
