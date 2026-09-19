"""What every feature router needs from the composition root, and the command path.

``create_app`` builds one :class:`RouteContext` and hands it to each router
factory in ``routes/``; nothing here knows about FastAPI wiring beyond the
dependency callables it is given. :class:`CommandExecutor` is the Phase 2
resilience wrapper around the command service (breaker + retry, storage
faults and schema skew mapped to 503 with ``Retry-After``) that every
command route shares -- moved out of ``create_app`` unchanged.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import Any
from uuid import UUID

from fastapi import HTTPException
from fastapi.responses import JSONResponse

from cloudscale.application.command_service import CommandService
from cloudscale.application.ports import AccountRegistry
from cloudscale.application.query_service import QueryService
from cloudscale.domain.commands import AccountCommand
from cloudscale.domain.results import CommandResult
from cloudscale.domain.upcasting import UnknownSchemaVersionError
from cloudscale.entrypoints.http.auth import Principal
from cloudscale.entrypoints.http.models import CommandResponse, PostingResponse
from cloudscale.entrypoints.http.observability import Metrics, audit_command
from cloudscale.entrypoints.http.settings import HttpSettings
from cloudscale.resilience import (
    CircuitBreaker,
    CircuitOpenError,
    RetryPolicy,
    call_with_retry,
)

API_VERSION = "v1"
_LOGGER = logging.getLogger("cloudscale.http")

#: Documented non-2xx responses shared by the authenticated account routes.
#: The bodies are described in docs/API_ERRORS.md; declaring them here makes
#: the committed contract (docs/openapi.json) honest for client generators.
AUTH_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: {"description": "Missing, invalid or expired bearer token (fixed message)."},
    403: {"description": "Token valid but not authorized for this account."},
    429: {"description": "Rate limit exceeded; honour Retry-After."},
    503: {
        "description": "Command path unavailable; retry with the SAME command_id after Retry-After."
    },
}


def unavailable(reason: str, retry_after_seconds: int) -> HTTPException:
    """503 for the command path with the cause a client may log and a Retry-After."""
    return HTTPException(
        status_code=503,
        detail=f"command path unavailable ({reason})",
        headers={"Retry-After": str(retry_after_seconds)},
    )


def command_response(
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


class CommandExecutor:
    """Run one command through breaker + retry; map storage faults to 503."""

    def __init__(
        self,
        command_service: CommandService,
        *,
        retry_policy: RetryPolicy,
        breaker: CircuitBreaker,
        transient_errors: tuple[type[BaseException], ...],
        metrics: Metrics,
    ) -> None:
        self._service = command_service
        self._retry = retry_policy
        self._breaker = breaker
        self._transient = transient_errors
        self._metrics = metrics

    def execute(
        self,
        command: AccountCommand,
        *,
        command_id: UUID,
        principal: Principal,
        command_type: str,
    ) -> CommandResult:
        def _attempt() -> CommandResult:
            return self._breaker.call(
                partial(
                    self._service.execute,
                    command,
                    command_id=command_id,
                    issuer=principal.issuer,
                    subject=principal.subject,
                )
            )

        try:
            result = call_with_retry(
                _attempt, self._retry, retryable_errors=self._transient
            )
        except CircuitOpenError as error:
            retry_after = max(1, int(error.retry_after_seconds))
            raise unavailable("circuit open", retry_after) from error
        except self._transient as error:
            raise unavailable("transient storage failure", 1) from error
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
            raise unavailable("event schema newer than this build", 60) from error

        audit_command(
            subject=principal.subject,
            issuer=principal.issuer,
            account_id=command.account_id,
            command_type=command_type,
            command_id=command_id,
            result=result,
        )
        self._metrics.observe_command(result)
        return result


@dataclass(frozen=True)
class RouteContext:
    """Everything a feature router needs, built once by ``create_app``.

    ``authenticated`` is the per-request dependency (verify token, then the
    per-subject rate limit); ``query_auth`` is the same or a no-op when reads
    are explicitly relaxed. Routers take the dependency *callables* and wrap
    them in ``Depends`` themselves.
    """

    settings: HttpSettings
    executor: CommandExecutor
    query_service: QueryService
    account_registry: AccountRegistry | None
    authenticated: Callable[..., Principal]
    query_auth: Callable[..., Any]
    metrics: Metrics

    def may_read(self, principal: Principal, account_id: str) -> bool:
        if principal.may_access(account_id):
            return True
        return (
            self.account_registry is not None
            and self.account_registry.owner_of(account_id) == principal.subject
        )


__all__ = [
    "API_VERSION",
    "AUTH_RESPONSES",
    "CommandExecutor",
    "RouteContext",
    "command_response",
    "unavailable",
]
