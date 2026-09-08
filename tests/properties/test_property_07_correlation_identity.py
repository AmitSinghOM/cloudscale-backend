"""Feature: cloudscale-production-readiness, Property 7.

Property 7: Correlation identity is total.
Caller-supplied valid correlation IDs are preserved. When an ID is omitted, the
correlation contract resolves one ID once and shares it between the accepted
command result and event envelope.

Task 1.10 dependency: production command orchestration does not exist yet. The
omitted-ID branch therefore models only the resolution boundary immediately
before the task-1.6 domain constructors; it does not claim HTTP orchestration
coverage and must be exercised through that production seam when task 1.10 lands.

Validates: Requirements 3.7, 5.3.
"""

from collections.abc import Callable
from datetime import UTC, datetime
import hashlib
from uuid import UUID

from hypothesis import given, settings
from hypothesis import strategies as st

from cloudscale.domain.events import Deposited, EventEnvelope
from cloudscale.domain.results import CommandOutcome, CommandResult

_EVENT_ID = UUID("00000000-0000-4000-8000-000000000001")
_COMMAND_ID = UUID("00000000-0000-4000-8000-000000000002")
_OCCURRED_AT = datetime(2026, 3, 9, 12, 0, tzinfo=UTC)
_REQUEST_HASH = hashlib.sha256(b"property-07-normalized-command").digest()


def _resolve_correlation_id(
    supplied_id: UUID | None,
    generate_id: Callable[[], UUID],
) -> UUID:
    """Model the task-1.10 boundary without adding production orchestration."""

    return supplied_id if supplied_id is not None else generate_id()


def _accepted_artifacts(correlation_id: UUID) -> tuple[CommandResult, EventEnvelope]:
    """Construct the task-1.6 result/event pair at the lowest stable seam."""

    event = EventEnvelope.from_domain_event(
        Deposited(account_id="account-property-07", amount=1),
        event_id=_EVENT_ID,
        stream_version=1,
        occurred_at=_OCCURRED_AT,
        correlation_id=correlation_id,
        causation_id=_COMMAND_ID,
        command_id=_COMMAND_ID,
    )
    result = CommandResult(
        command_id=_COMMAND_ID,
        request_hash=_REQUEST_HASH,
        outcome=CommandOutcome.ACCEPTED,
        account_id="account-property-07",
        expected_version=0,
        current_version=0,
        committed_version=1,
        event_id=_EVENT_ID,
        correlation_id=correlation_id,
        error_code=None,
        http_status=201,
        created_at=_OCCURRED_AT,
    )
    return result, event


@settings(max_examples=100)
@given(supplied_id=st.uuids(), unused_generated_id=st.uuids())
def test_supplied_valid_correlation_id_is_preserved(
    supplied_id: UUID,
    unused_generated_id: UUID,
) -> None:
    """Property 7: a caller-supplied UUID is shared without regeneration."""

    generation_count = 0

    def generate_id() -> UUID:
        nonlocal generation_count
        generation_count += 1
        return unused_generated_id

    resolved_id = _resolve_correlation_id(supplied_id, generate_id)
    result, event = _accepted_artifacts(resolved_id)

    assert generation_count == 0
    assert resolved_id == supplied_id
    assert result.correlation_id == supplied_id
    assert event.correlation_id == supplied_id
    assert result.correlation_id == event.correlation_id


@settings(max_examples=100)
@given(generated_id=st.uuids())
def test_omitted_correlation_id_is_generated_once_and_shared(
    generated_id: UUID,
) -> None:
    """Property 7: an omitted UUID resolves once for both domain records."""

    generation_count = 0

    def generate_id() -> UUID:
        nonlocal generation_count
        generation_count += 1
        return generated_id

    resolved_id = _resolve_correlation_id(None, generate_id)
    result, event = _accepted_artifacts(resolved_id)

    assert generation_count == 1
    assert resolved_id == generated_id
    assert result.correlation_id == generated_id
    assert event.correlation_id == generated_id
    assert result.correlation_id == event.correlation_id
