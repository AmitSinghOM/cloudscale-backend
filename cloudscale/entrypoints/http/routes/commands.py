"""Money movement: single-account commands, transfers (ADR-0011), posting sets (ADR-0013)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from cloudscale.domain.commands import Deposit, Leg, Post, Transfer, Withdraw
from cloudscale.domain.errors import DomainError
from cloudscale.entrypoints.http.auth import Principal, authorize_account
from cloudscale.entrypoints.http.context import (
    API_VERSION,
    AUTH_RESPONSES,
    RouteContext,
    command_response,
)
from cloudscale.entrypoints.http.models import (
    CommandRequest,
    PostingsRequest,
    TransferRequest,
)


def build_router(ctx: RouteContext) -> APIRouter:
    router = APIRouter()

    @router.post(
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
            **AUTH_RESPONSES,
        },
    )
    def post_command(
        account_id: str,
        request: CommandRequest,
        principal: Principal = Depends(ctx.authenticated),  # writes ALWAYS authenticate
    ) -> JSONResponse:
        authorize_account(principal, account_id, ctx.account_registry)
        try:
            command = (
                Deposit(account_id, request.amount, request.expected_version)
                if request.type == "deposit"
                else Withdraw(account_id, request.amount, request.expected_version)
            )
        except DomainError as error:
            raise HTTPException(status_code=400, detail=error.code) from error
        result = ctx.executor.execute(
            command,
            command_id=request.command_id,
            principal=principal,
            command_type=request.type,
        )
        return command_response(result)

    @router.post(
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
            **AUTH_RESPONSES,
        },
    )
    def post_transfer(
        account_id: str,
        request: TransferRequest,
        principal: Principal = Depends(ctx.authenticated),
    ) -> JSONResponse:
        """Move funds from the path account to ``target_account_id`` (ADR-0011).

        Authorization is on the source only: money leaves the caller's
        account; the target may be any account, exactly as a deposit may
        create one. The target posting's ``committed_version`` is redacted
        unless the caller may read that account.
        """
        authorize_account(principal, account_id, ctx.account_registry)
        try:
            command = Transfer(
                account_id,
                request.target_account_id,
                request.amount,
                request.expected_version,
            )
        except DomainError as error:
            raise HTTPException(status_code=400, detail=error.code) from error
        result = ctx.executor.execute(
            command,
            command_id=request.command_id,
            principal=principal,
            command_type="transfer",
        )
        return command_response(
            result, may_read=lambda target: ctx.may_read(principal, target)
        )

    @router.post(
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
            **AUTH_RESPONSES,
        },
    )
    def post_postings(
        account_id: str,
        request: PostingsRequest,
        principal: Principal = Depends(ctx.authenticated),
    ) -> JSONResponse:
        """Commit a balanced N-leg posting set in one transaction (ADR-0013).

        Authorization is on the anchor (the path account), which must be a
        debited leg. Every other leg's ``committed_version`` is redacted unless
        the caller may read that account.
        """
        authorize_account(principal, account_id, ctx.account_registry)
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
        result = ctx.executor.execute(
            command,
            command_id=request.command_id,
            principal=principal,
            command_type="post",
        )
        return command_response(
            result, may_read=lambda target: ctx.may_read(principal, target)
        )

    # -- holds (ADR-0014) ---------------------------------------------------------

    return router


__all__ = ["build_router"]
