"""Request and response models of the HTTP contract (docs/openapi.json).

Moved out of ``app.py`` unchanged so each feature router can import only the
shapes it serves; ``app.py`` re-exports them for existing importers.
"""

from __future__ import annotations

from typing import Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class Closeable(Protocol):
    def close(self) -> None: ...


class ReadinessProbe(Protocol):
    """Answers "can this replica serve traffic right now?".

    Must touch the real dependencies (a storage round-trip, the schema
    revision) and raise on any failure; the caller maps exceptions to 503.
    Returns a small dict of what was checked, for the response body.
    """

    def check(self) -> dict[str, object]: ...


class ReadyResponse(BaseModel):
    status: Literal["ready", "not_ready"]
    checks: dict[str, object]


class RegisterAccountRequest(BaseModel):
    """Claim ownership of an account id for the authenticated caller."""

    model_config = ConfigDict(extra="forbid")

    account_id: str


class CommandRequest(BaseModel):
    """One account command; ``command_id`` is the caller-owned idempotency key."""

    # Unknown fields are a client bug (e.g. a misspelled expected_version);
    # surface them as 422 instead of silently ignoring them.
    model_config = ConfigDict(extra="forbid")

    command_id: UUID
    type: Literal["deposit", "withdraw"]
    amount: int
    expected_version: int


class TransferRequest(BaseModel):
    """Move ``amount`` from the path account to ``target_account_id`` (ADR-0011).

    ``expected_version`` is the source stream's version; the target has no
    client-supplied guard.
    """

    model_config = ConfigDict(extra="forbid")

    command_id: UUID
    target_account_id: str
    amount: int
    expected_version: int


class LegRequest(BaseModel):
    """One leg of a posting set (ADR-0013)."""

    model_config = ConfigDict(extra="forbid")

    account_id: str
    amount: int
    direction: Literal["debit", "credit"]


class PostingsRequest(BaseModel):
    """Commit 2..MAX_LEGS balanced legs atomically; the path account is the anchor.

    The anchor must be one of the debited legs and is the only stream whose
    ``expected_version`` the caller supplies. Validation (balance, duplicate
    accounts, leg count, anchor debited) is the domain's; failures are 400s
    with the domain code.
    """

    model_config = ConfigDict(extra="forbid")

    command_id: UUID
    postings: list[LegRequest]
    expected_version: int


class HoldRequest(BaseModel):
    """Reserve funds on the path account for ``target_account_id`` (ADR-0014).

    ``ttl_seconds`` (bounded by ``CLOUDSCALE_HOLD_MAX_TTL_SECONDS``) is part of
    the idempotent request; the decision stamps the absolute ``expires_at``
    from its clock. The hold id is the ``command_id``.
    """

    model_config = ConfigDict(extra="forbid")

    command_id: UUID
    target_account_id: str
    amount: int
    expected_version: int
    ttl_seconds: int


class PostHoldRequest(BaseModel):
    """Settle a hold; ``amount`` omitted captures the full held amount."""

    model_config = ConfigDict(extra="forbid")

    command_id: UUID
    expected_version: int
    amount: int | None = None


class ReleaseHoldRequest(BaseModel):
    """Void an open hold."""

    model_config = ConfigDict(extra="forbid")

    command_id: UUID
    expected_version: int


class PostingResponse(BaseModel):
    """One stream an accepted command wrote.

    ``committed_version`` is present only for accounts the caller may read: a
    stream version counts that account's activity, and a transfer's target
    is not necessarily the caller's account.
    """

    account_id: str
    event_id: UUID
    committed_version: int | None


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
    postings: list[PostingResponse]


class BalanceResponse(BaseModel):
    account_id: str
    balance: int
    #: Sum of this account's open holds (ADR-0014); ``available`` is what a
    #: debit may use.
    held: int
    available: int
    version: int
    #: Read models are projections of the log; reads can trail writes.
    consistency: str = "eventual"


class HealthResponse(BaseModel):
    status: str
    storage: dict[str, object]
