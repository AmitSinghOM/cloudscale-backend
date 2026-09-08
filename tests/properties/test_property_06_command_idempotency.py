"""Feature: cloudscale-production-readiness, Property 6.

Property 6: Command processing is idempotent and collision-safe.
Equivalent normalized payloads sharing a command ID return the immutable persisted
result without another event. Differing normalized payloads sharing a command ID
return the same stable conflict without changing persisted state.

Validates: Requirements 3.4, 3.5, 3.6.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from hypothesis import given, settings
from hypothesis import strategies as st

from cloudscale.application.command_service import CommandService
from cloudscale.application.ports import NormalizedCommand
from cloudscale.domain.commands import (
    MAX_SIGNED_BIGINT,
    AccountCommand,
    Deposit,
    Withdraw,
)
from cloudscale.domain.results import CommandOutcome, CommandResult

_CREATED_AT = datetime(2026, 3, 10, 12, 0, tzinfo=UTC)
_EVENT_ID = UUID("00000000-0000-4000-8000-000000000006")
_SAFE_TEXT = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",)),
    min_size=1,
    max_size=32,
)
_AMOUNT = st.integers(min_value=1, max_value=MAX_SIGNED_BIGINT)
_EXPECTED_VERSION = st.integers(min_value=0, max_value=MAX_SIGNED_BIGINT)


@dataclass(frozen=True, slots=True)
class _CommandCase:
    command: AccountCommand
    issuer: str
    subject: str


class _IdempotencyModel:
    """Focused in-memory implementation of the task-1.10 command port contract."""

    def __init__(self) -> None:
        self.results: dict[UUID, tuple[bytes, CommandResult]] = {}
        self.events: list[bytes] = []
        self.requests: list[NormalizedCommand] = []

    def execute(self, request: NormalizedCommand) -> CommandResult:
        self.requests.append(request)
        persisted = self.results.get(request.command_id)
        if persisted is not None:
            canonical_payload, original_result = persisted
            if request.canonical_payload == canonical_payload:
                return original_result
            return self._conflict(request)

        result = CommandResult(
            command_id=request.command_id,
            request_hash=request.request_hash,
            outcome=CommandOutcome.ACCEPTED,
            account_id=request.command.account_id,
            expected_version=request.command.expected_version,
            current_version=request.command.expected_version,
            committed_version=request.command.expected_version + 1,
            event_id=_EVENT_ID,
            correlation_id=request.correlation_id,
            error_code=None,
            http_status=201,
            created_at=_CREATED_AT,
        )
        self.events.append(request.canonical_payload)
        self.results[request.command_id] = (request.canonical_payload, result)
        return result

    @staticmethod
    def _conflict(request: NormalizedCommand) -> CommandResult:
        return CommandResult(
            command_id=request.command_id,
            request_hash=request.request_hash,
            outcome=CommandOutcome.COMMAND_ID_CONFLICT,
            account_id=request.command.account_id,
            expected_version=request.command.expected_version,
            current_version=request.command.expected_version,
            committed_version=None,
            event_id=None,
            correlation_id=request.correlation_id,
            error_code="command_id_conflict",
            http_status=409,
            created_at=_CREATED_AT,
        )

    def persisted_state(
        self,
    ) -> tuple[tuple[tuple[UUID, bytes, str], ...], tuple[bytes, ...]]:
        """Return mutation-relevant state, excluding request-observation counters."""

        results = tuple(
            sorted(
                (
                    command_id,
                    payload,
                    result.to_json(),
                )
                for command_id, (payload, result) in self.results.items()
            )
        )
        return results, tuple(self.events)


@st.composite
def _command_cases(draw: st.DrawFn) -> _CommandCase:
    account_id = draw(_SAFE_TEXT)
    amount = draw(_AMOUNT)
    expected_version = draw(
        _EXPECTED_VERSION.filter(lambda value: value < MAX_SIGNED_BIGINT)
    )
    command_type = draw(st.sampled_from((Deposit, Withdraw)))
    return _CommandCase(
        command=command_type(account_id, amount, expected_version),
        issuer=draw(_SAFE_TEXT),
        subject=draw(_SAFE_TEXT),
    )


def _replace_command_field(case: _CommandCase, field: str) -> _CommandCase:
    command = case.command
    command_type = type(command)
    account_id = command.account_id
    amount = command.amount
    expected_version = command.expected_version
    issuer = case.issuer
    subject = case.subject

    if field == "type":
        command_type = Withdraw if command_type is Deposit else Deposit
    elif field == "account_id":
        account_id += "\x00"
    elif field == "amount":
        amount = amount - 1 if amount == MAX_SIGNED_BIGINT else amount + 1
    elif field == "expected_version":
        expected_version += 1
    elif field == "issuer":
        issuer += "\x00"
    elif field == "subject":
        subject += "\x00"
    else:  # pragma: no cover - the strategy enumerates every supported field
        raise AssertionError(f"unsupported normalized field: {field}")

    return _CommandCase(
        command=command_type(account_id, amount, expected_version),
        issuer=issuer,
        subject=subject,
    )


@settings(max_examples=100)
@given(
    case=_command_cases(),
    command_id=st.uuids(),
    correlation_ids=st.lists(st.uuids(), min_size=2, max_size=5, unique=True),
)
def test_equivalent_retries_return_persisted_result_without_another_event(
    case: _CommandCase,
    command_id: UUID,
    correlation_ids: list[UUID],
) -> None:
    """Property 6: equivalent retries replay one original result and event."""

    model = _IdempotencyModel()
    service = CommandService(model)
    original = service.execute(
        case.command,
        command_id=command_id,
        correlation_id=correlation_ids[0],
        issuer=case.issuer,
        subject=case.subject,
    )
    persisted_after_original = model.persisted_state()

    for correlation_id in correlation_ids[1:]:
        equivalent_command = type(case.command)(
            case.command.account_id,
            case.command.amount,
            case.command.expected_version,
        )
        replay = service.execute(
            equivalent_command,
            command_id=command_id,
            correlation_id=correlation_id,
            issuer=case.issuer,
            subject=case.subject,
        )

        assert replay is original
        assert replay.correlation_id == correlation_ids[0]
        assert model.persisted_state() == persisted_after_original

    assert len(model.events) == 1
    assert len(model.results) == 1
    assert {request.canonical_payload for request in model.requests} == {
        model.requests[0].canonical_payload
    }
    assert {request.request_hash for request in model.requests} == {
        original.request_hash
    }


@settings(max_examples=100)
@given(
    case=_command_cases(),
    changed_field=st.sampled_from(
        ("type", "account_id", "amount", "expected_version", "issuer", "subject")
    ),
    command_id=st.uuids(),
    correlation_id=st.uuids(),
)
def test_differing_payloads_conflict_deterministically_without_mutation(
    case: _CommandCase,
    changed_field: str,
    command_id: UUID,
    correlation_id: UUID,
) -> None:
    """Property 6: a differing normalized payload conflicts without mutation."""

    model = _IdempotencyModel()
    service = CommandService(model)
    original = service.execute(
        case.command,
        command_id=command_id,
        correlation_id=correlation_id,
        issuer=case.issuer,
        subject=case.subject,
    )
    persisted_after_original = model.persisted_state()
    different = _replace_command_field(case, changed_field)

    first_conflict = service.execute(
        different.command,
        command_id=command_id,
        correlation_id=correlation_id,
        issuer=different.issuer,
        subject=different.subject,
    )
    second_conflict = service.execute(
        different.command,
        command_id=command_id,
        correlation_id=correlation_id,
        issuer=different.issuer,
        subject=different.subject,
    )

    original_request, conflicting_request = model.requests[:2]
    assert conflicting_request.canonical_payload != original_request.canonical_payload
    assert conflicting_request.request_hash != original_request.request_hash
    assert first_conflict == second_conflict
    assert first_conflict.outcome is CommandOutcome.COMMAND_ID_CONFLICT
    assert first_conflict.error_code == "command_id_conflict"
    assert first_conflict.http_status == 409
    assert model.results[command_id][1] is original
    assert model.persisted_state() == persisted_after_original
    assert len(model.events) == 1
    assert len(model.results) == 1
