"""Money movement: commands, transfers (ADR-0011), posting sets (ADR-0013), reverts (ADR-0015)."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from cloudscale.domain.commands import Deposit, Leg, Post, Revert, Transfer, Withdraw
from cloudscale.domain.errors import DomainError
from cloudscale.domain.events import BALANCE_SIGN
from cloudscale.entrypoints.http.auth import ADMIN_SCOPE, Principal, authorize_account
from cloudscale.entrypoints.http.context import (
    API_VERSION,
    AUTH_RESPONSES,
    RouteContext,
    command_response,
)
from cloudscale.entrypoints.http.models import (
    CommandRequest,
    PostingsRequest,
    RevertRequest,
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

    @router.post(
        f"/{API_VERSION}/accounts/{{account_id}}/transfers/{{transfer_id}}/revert",
        status_code=201,
        responses={
            400: {
                "description": (
                    "CommandResult with outcome=domain_rejected and error_code: "
                    "not_revertible (cash movements, hold placements, unknown id) or "
                    "anchor_not_credited."
                )
            },
            409: {
                "description": (
                    "CommandResult: already_reverted, version_conflict (anchor stream) "
                    "or command_id_conflict."
                )
            },
            422: {
                "description": (
                    "CommandResult: insufficient_funds -- a payee no longer has the "
                    "money available; nothing was appended."
                )
            },
            **AUTH_RESPONSES,
        },
    )
    def post_revert(
        account_id: str,
        transfer_id: UUID,
        request: RevertRequest,
        principal: Principal = Depends(ctx.authenticated),
    ) -> JSONResponse:
        """Append the mirror of a committed posting set (ADR-0015).

        Authorization is on the accounts the revert DEBITS (every original
        payee) or the admin scope -- never the anchor alone, or a payer could
        claw back a payment unilaterally. The debit set is read from the log,
        not the read model, so a just-committed set can be reverted at once.
        """
        authorize_account(principal, account_id, ctx.account_registry)
        legs = ctx.executor.legs_of(transfer_id)
        if ADMIN_SCOPE not in principal.scopes and not any(
            ctx.may_read(principal, leg.account_id) for leg in legs
        ):
            # Party to none of the legs (or no such set): the same 403 either way,
            # so an anchor-only token cannot probe whether foreign payments exist.
            raise HTTPException(
                status_code=403, detail="principal is not authorized for this account"
            )
        for leg in legs:
            if BALANCE_SIGN[type(leg).__name__] > 0:  # they received it: we debit them
                authorize_account(principal, leg.account_id, ctx.account_registry)
        try:
            command = Revert(account_id, transfer_id, request.expected_version)
        except DomainError as error:
            raise HTTPException(status_code=400, detail=error.code) from error
        result = ctx.executor.execute(
            command,
            command_id=request.command_id,
            principal=principal,
            command_type="revert",
        )
        return command_response(
            result, may_read=lambda target: ctx.may_read(principal, target)
        )

    return router


__all__ = ["build_router"]
