"""Pending transfers (ADR-0014): place, post, void."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from cloudscale.domain.commands import Hold, PostHold, VoidHold
from cloudscale.domain.errors import DomainError
from cloudscale.entrypoints.http.auth import Principal, authorize_account
from cloudscale.entrypoints.http.context import (
    API_VERSION,
    AUTH_RESPONSES,
    RouteContext,
    command_response,
)
from cloudscale.entrypoints.http.models import (
    HoldRequest,
    PostHoldRequest,
    ReleaseHoldRequest,
)


def build_router(ctx: RouteContext) -> APIRouter:
    router = APIRouter()

    _HOLD_RESPONSES: dict[int | str, dict[str, Any]] = {
        400: {
            "description": (
                "CommandResult with outcome=domain_rejected and error_code "
                "(hold_not_open, hold_expired, capture_exceeds_hold, same_account, "
                "amount_out_of_range), or an invalid ttl_seconds."
            )
        },
        409: {
            "description": (
                "CommandResult: version_conflict (source stream) or command_id_conflict."
            )
        },
        422: {
            "description": "CommandResult: insufficient_funds (available, not balance)."
        },
        **AUTH_RESPONSES,
    }

    @router.post(
        f"/{API_VERSION}/accounts/{{account_id}}/holds",
        status_code=201,
        responses=_HOLD_RESPONSES,
    )
    def post_hold(
        account_id: str,
        request: HoldRequest,
        principal: Principal = Depends(ctx.authenticated),
    ) -> JSONResponse:
        """Reserve funds now for a later posting (ADR-0014). The hold id is the command id.

        Checked against AVAILABLE funds (balance minus open holds). The target
        learns nothing until the hold is posted; its stream is untouched.
        """
        authorize_account(principal, account_id, ctx.account_registry)
        if not 1 <= request.ttl_seconds <= ctx.settings.hold_max_ttl_seconds:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"ttl_seconds must be between 1 and {ctx.settings.hold_max_ttl_seconds} "
                    "(CLOUDSCALE_HOLD_MAX_TTL_SECONDS)"
                ),
            )
        try:
            command = Hold(
                account_id,
                request.target_account_id,
                request.amount,
                request.expected_version,
                request.ttl_seconds,
            )
        except DomainError as error:
            raise HTTPException(status_code=400, detail=error.code) from error
        result = ctx.executor.execute(
            command,
            command_id=request.command_id,
            principal=principal,
            command_type="hold",
        )
        return command_response(result)

    @router.post(
        f"/{API_VERSION}/accounts/{{account_id}}/holds/{{hold_id}}/post",
        status_code=201,
        responses=_HOLD_RESPONSES,
    )
    def post_hold_capture(
        account_id: str,
        hold_id: UUID,
        request: PostHoldRequest,
        principal: Principal = Depends(ctx.authenticated),
    ) -> JSONResponse:
        """Settle an open hold: debit the source, credit the hold's target (ADR-0014).

        A smaller ``amount`` captures part and releases the remainder in the
        same transaction. The target posting's ``committed_version`` is
        redacted unless the caller may read that account.
        """
        authorize_account(principal, account_id, ctx.account_registry)
        try:
            command = PostHold(
                account_id, hold_id, request.expected_version, request.amount
            )
        except DomainError as error:
            raise HTTPException(status_code=400, detail=error.code) from error
        result = ctx.executor.execute(
            command,
            command_id=request.command_id,
            principal=principal,
            command_type="post_hold",
        )
        return command_response(
            result, may_read=lambda target: ctx.may_read(principal, target)
        )

    @router.post(
        f"/{API_VERSION}/accounts/{{account_id}}/holds/{{hold_id}}/void",
        status_code=201,
        responses=_HOLD_RESPONSES,
    )
    def post_hold_void(
        account_id: str,
        hold_id: UUID,
        request: ReleaseHoldRequest,
        principal: Principal = Depends(ctx.authenticated),
    ) -> JSONResponse:
        """Release an open hold without moving funds (ADR-0014)."""
        authorize_account(principal, account_id, ctx.account_registry)
        try:
            command = VoidHold(account_id, hold_id, request.expected_version)
        except DomainError as error:
            raise HTTPException(status_code=400, detail=error.code) from error
        result = ctx.executor.execute(
            command,
            command_id=request.command_id,
            principal=principal,
            command_type="void_hold",
        )
        return command_response(result)

    return router


__all__ = ["build_router"]
