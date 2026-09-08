"""Fake-port tests for command normalization and atomic orchestration.

Validates: Requirements 2.1-2.4, 2.7, 3.4-3.7.
"""

import hashlib
import json
from datetime import UTC, datetime
from uuid import UUID

import pytest

from cloudscale.application.command_service import CommandService
from cloudscale.application.ports import NormalizedCommand
from cloudscale.domain.account import AccountState, apply, decide
from cloudscale.domain.commands import Deposit, Withdraw
from cloudscale.domain.errors import DomainError, InsufficientFundsError
from cloudscale.domain.results import CommandOutcome, CommandResult

COMMAND_ID = UUID("00000000-0000-4000-8000-000000000001")
OTHER_COMMAND_ID = UUID("00000000-0000-4000-8000-000000000002")
THIRD_COMMAND_ID = UUID("00000000-0000-4000-8000-000000000003")
CORRELATION_ID = UUID("00000000-0000-4000-8000-000000000011")
OTHER_CORRELATION_ID = UUID("00000000-0000-4000-8000-000000000012")
CREATED_AT = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)


class FakeCommandUnitOfWork:
    """Small atomic fake that makes the port's idempotency contract observable."""

    def __init__(self) -> None:
        self.results: dict[UUID, CommandResult] = {}
        self.states: dict[str, AccountState] = {}
        self.events: list[object] = []
        self.execute_calls: list[NormalizedCommand] = []
        self.projection_reads = 0
        self.projection_updates = 0
        self._next_event_id = 100

    def get_balance(self, account_id: str) -> None:
        self.projection_reads += 1
        return None

    def apply(self, delivery: object) -> None:
        self.projection_updates += 1

    def execute(self, request: NormalizedCommand) -> CommandResult:
        self.execute_calls.append(request)
        existing = self.results.get(request.command_id)
        state = self.states.get(request.command.account_id, AccountState())
        if existing is not None:
            if existing.request_hash == request.request_hash:
                return existing
            return self._rejection(
                request,
                CommandOutcome.COMMAND_ID_CONFLICT,
                "command_id_conflict",
                409,
                state.version,
            )

        if request.command.expected_version != state.version:
            result = self._rejection(
                request,
                CommandOutcome.VERSION_CONFLICT,
                "version_conflict",
                409,
                state.version,
            )
            self.results[request.command_id] = result
            return result

        try:
            event = decide(state, request.command)
        except InsufficientFundsError:
            result = self._rejection(
                request,
                CommandOutcome.INSUFFICIENT_FUNDS,
                "insufficient_funds",
                422,
                state.version,
            )
            self.results[request.command_id] = result
            return result
        except DomainError as error:
            result = self._rejection(
                request,
                CommandOutcome.DOMAIN_REJECTED,
                error.code,
                400,
                state.version,
            )
            self.results[request.command_id] = result
            return result

        next_state = apply(state, event)
        event_id = UUID(int=self._next_event_id)
        self._next_event_id += 1
        result = CommandResult(
            command_id=request.command_id,
            request_hash=request.request_hash,
            outcome=CommandOutcome.ACCEPTED,
            account_id=request.command.account_id,
            expected_version=request.command.expected_version,
            current_version=state.version,
            committed_version=next_state.version,
            event_id=event_id,
            correlation_id=request.correlation_id,
            error_code=None,
            http_status=201,
            created_at=CREATED_AT,
        )
        self.states[request.command.account_id] = next_state
        self.events.append(event)
        self.results[request.command_id] = result
        return result

    @staticmethod
    def _rejection(
        request: NormalizedCommand,
        outcome: CommandOutcome,
        error_code: str,
        http_status: int,
        current_version: int,
    ) -> CommandResult:
        return CommandResult(
            command_id=request.command_id,
            request_hash=request.request_hash,
            outcome=outcome,
            account_id=request.command.account_id,
            expected_version=request.command.expected_version,
            current_version=current_version,
            committed_version=None,
            event_id=None,
            correlation_id=request.correlation_id,
            error_code=error_code,
            http_status=http_status,
            created_at=CREATED_AT,
        )


def _service(fake: FakeCommandUnitOfWork) -> CommandService:
    return CommandService(fake, correlation_id_factory=lambda: CORRELATION_ID)


def test_normalization_is_canonical_utf8_json_and_sha256() -> None:
    fake = FakeCommandUnitOfWork()

    _service(fake).execute(
        Deposit("काता", 25, expected_version=0),
        command_id=COMMAND_ID,
        correlation_id=CORRELATION_ID,
        issuer="https://issuer.example",
        subject="subject-1",
    )

    request = fake.execute_calls[0]
    decoded = request.canonical_payload.decode("utf-8")
    assert "काता" in decoded
    assert request.request_hash == hashlib.sha256(request.canonical_payload).digest()
    assert decoded == json.dumps(
        json.loads(decoded),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    assert "command_id" not in decoded
    assert "correlation_id" not in decoded


def test_equivalent_retry_returns_original_result_without_duplicate_append() -> None:
    fake = FakeCommandUnitOfWork()
    service = _service(fake)
    command = Deposit("account-1", 25, expected_version=0)

    original = service.execute(
        command,
        command_id=COMMAND_ID,
        correlation_id=CORRELATION_ID,
        issuer="issuer",
        subject="subject",
    )
    replay = service.execute(
        command,
        command_id=COMMAND_ID,
        correlation_id=OTHER_CORRELATION_ID,
        issuer="issuer",
        subject="subject",
    )

    assert replay is original
    assert replay.correlation_id == CORRELATION_ID
    assert len(fake.events) == 1
    assert len(fake.results) == 1


@pytest.mark.parametrize(
    ("issuer", "subject"),
    [("other-issuer", "subject"), ("issuer", "other-subject")],
)
def test_command_identity_is_scoped_by_exact_issuer_and_subject(
    issuer: str, subject: str
) -> None:
    fake = FakeCommandUnitOfWork()
    service = _service(fake)
    command = Deposit("account-1", 25, expected_version=0)
    original = service.execute(
        command,
        command_id=COMMAND_ID,
        correlation_id=CORRELATION_ID,
        issuer="issuer",
        subject="subject",
    )

    collision = service.execute(
        command,
        command_id=COMMAND_ID,
        correlation_id=OTHER_CORRELATION_ID,
        issuer=issuer,
        subject=subject,
    )

    assert collision.outcome is CommandOutcome.COMMAND_ID_CONFLICT
    assert collision.error_code == "command_id_conflict"
    assert collision.http_status == 409
    assert fake.results[COMMAND_ID] is original
    assert len(fake.events) == 1


def test_different_payload_with_same_command_id_conflicts_without_append() -> None:
    fake = FakeCommandUnitOfWork()
    service = _service(fake)
    original = service.execute(
        Deposit("account-1", 25, 0),
        command_id=COMMAND_ID,
        issuer="issuer",
        subject="subject",
    )

    collision = service.execute(
        Deposit("account-1", 26, 0),
        command_id=COMMAND_ID,
        issuer="issuer",
        subject="subject",
    )

    assert collision.outcome is CommandOutcome.COMMAND_ID_CONFLICT
    assert fake.results[COMMAND_ID] is original
    assert len(fake.events) == 1


def test_version_conflict_persists_original_current_version() -> None:
    fake = FakeCommandUnitOfWork()
    service = _service(fake)
    service.execute(
        Deposit("account-1", 20, 0),
        command_id=COMMAND_ID,
        issuer="issuer",
        subject="subject",
    )
    conflict_command = Deposit("account-1", 5, 0)
    conflict = service.execute(
        conflict_command,
        command_id=OTHER_COMMAND_ID,
        issuer="issuer",
        subject="subject",
    )
    service.execute(
        Deposit("account-1", 1, 1),
        command_id=THIRD_COMMAND_ID,
        issuer="issuer",
        subject="subject",
    )

    replay = service.execute(
        conflict_command,
        command_id=OTHER_COMMAND_ID,
        issuer="issuer",
        subject="subject",
    )

    assert conflict.outcome is CommandOutcome.VERSION_CONFLICT
    assert conflict.current_version == 1
    assert replay is conflict
    assert replay.current_version == 1
    assert fake.states["account-1"].version == 2


def test_insufficient_funds_rejection_is_persisted_without_event() -> None:
    fake = FakeCommandUnitOfWork()
    service = _service(fake)
    command = Withdraw("account-1", 1, expected_version=0)

    original = service.execute(
        command,
        command_id=COMMAND_ID,
        issuer="issuer",
        subject="subject",
    )
    replay = service.execute(
        command,
        command_id=COMMAND_ID,
        issuer="issuer",
        subject="subject",
    )

    assert original.outcome is CommandOutcome.INSUFFICIENT_FUNDS
    assert replay is original
    assert fake.events == []
    assert fake.results[COMMAND_ID] is original


def test_correlation_identity_is_generated_once_or_preserved() -> None:
    fake = FakeCommandUnitOfWork()
    generated_ids: list[UUID] = []

    def generate() -> UUID:
        generated_ids.append(CORRELATION_ID)
        return CORRELATION_ID

    service = CommandService(fake, correlation_id_factory=generate)
    generated = service.execute(
        Deposit("account-1", 1, 0),
        command_id=COMMAND_ID,
        issuer="issuer",
        subject="subject",
    )
    preserved = service.execute(
        Deposit("account-2", 1, 0),
        command_id=OTHER_COMMAND_ID,
        correlation_id=OTHER_CORRELATION_ID,
        issuer="issuer",
        subject="subject",
    )

    assert generated.correlation_id == CORRELATION_ID
    assert preserved.correlation_id == OTHER_CORRELATION_ID
    assert generated_ids == [CORRELATION_ID]


def test_command_path_never_reads_or_updates_projection() -> None:
    fake = FakeCommandUnitOfWork()

    result = _service(fake).execute(
        Deposit("account-1", 1, 0),
        command_id=COMMAND_ID,
        issuer="issuer",
        subject="subject",
    )

    assert result.accepted
    assert fake.projection_reads == 0
    assert fake.projection_updates == 0
