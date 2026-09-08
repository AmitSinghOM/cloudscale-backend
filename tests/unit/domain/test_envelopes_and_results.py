"""Unit examples for immutable event envelopes and command results.

Validates: Requirements 3.1-3.4, 3.8-3.9.
"""

import hashlib
import json
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest

from cloudscale.domain.events import Deposited, EventEnvelope, Withdrawn
from cloudscale.domain.results import CommandOutcome, CommandResult

EVENT_ID = UUID("00000000-0000-4000-8000-000000000001")
COMMAND_ID = UUID("00000000-0000-4000-8000-000000000002")
CORRELATION_ID = UUID("00000000-0000-4000-8000-000000000003")
OCCURRED_AT = datetime(2026, 3, 8, 12, 34, 56, 123456, tzinfo=UTC)
REQUEST_HASH = hashlib.sha256(b"normalized-command").digest()


def _envelope(**overrides: object) -> EventEnvelope:
    values: dict[str, object] = {
        "event_id": EVENT_ID,
        "stream_id": "account-1",
        "stream_version": 1,
        "event_type": "Deposited",
        "occurred_at": OCCURRED_AT,
        "correlation_id": CORRELATION_ID,
        "causation_id": COMMAND_ID,
        "command_id": COMMAND_ID,
        "schema_version": 1,
        "payload": {"account_id": "account-1", "amount": 25},
    }
    values.update(overrides)
    return EventEnvelope(**values)  # type: ignore[arg-type]


def _result(**overrides: object) -> CommandResult:
    values: dict[str, object] = {
        "command_id": COMMAND_ID,
        "request_hash": REQUEST_HASH,
        "outcome": CommandOutcome.ACCEPTED,
        "account_id": "account-1",
        "expected_version": 0,
        "current_version": 0,
        "committed_version": 1,
        "event_id": EVENT_ID,
        "correlation_id": CORRELATION_ID,
        "error_code": None,
        "http_status": 201,
        "created_at": OCCURRED_AT,
    }
    values.update(overrides)
    return CommandResult(**values)  # type: ignore[arg-type]


def test_typed_event_round_trip_preserves_every_protected_field() -> None:
    envelope = EventEnvelope.from_domain_event(
        Deposited("account-1", 25),
        event_id=EVENT_ID,
        stream_version=1,
        occurred_at=OCCURRED_AT,
        correlation_id=CORRELATION_ID,
        causation_id=COMMAND_ID,
        command_id=COMMAND_ID,
    )

    restored = EventEnvelope.from_json(envelope.to_json())

    assert restored == envelope
    assert restored.to_domain_event() == Deposited("account-1", 25)
    assert restored.event_id is EVENT_ID or restored.event_id == EVENT_ID
    assert restored.occurred_at == OCCURRED_AT
    assert restored.causation_id == restored.command_id


def test_withdrawn_event_uses_the_existing_task_1_3_domain_type() -> None:
    envelope = EventEnvelope.from_domain_event(
        Withdrawn(" Exact Account ", 7),
        event_id=EVENT_ID,
        stream_version=9,
        occurred_at=OCCURRED_AT,
        correlation_id=CORRELATION_ID,
        causation_id=COMMAND_ID,
        command_id=COMMAND_ID,
    )

    assert envelope.stream_id == " Exact Account "
    assert envelope.event_type == "Withdrawn"
    assert envelope.to_domain_event() == Withdrawn(" Exact Account ", 7)


def test_envelope_and_nested_payload_are_immutable() -> None:
    payload = {"account_id": "account-1", "amount": 25}
    envelope = _envelope(payload=payload)
    payload["amount"] = 99

    assert envelope.payload["amount"] == 25
    with pytest.raises(FrozenInstanceError):
        envelope.event_id = UUID(int=0)  # type: ignore[misc]
    with pytest.raises(TypeError):
        envelope.payload["amount"] = 99  # type: ignore[index]


def test_envelope_serialization_is_canonical_utf8_json() -> None:
    envelope = _envelope(stream_id="काता", payload={"account_id": "काता", "amount": 1})

    serialized = envelope.to_json()

    assert serialized == envelope.to_json()
    assert "काता" in serialized
    assert ": " not in serialized
    assert list(json.loads(serialized)) == sorted(json.loads(serialized))


@pytest.mark.parametrize(
    "occurred_at",
    [
        datetime(2026, 3, 8, 12),
        datetime(2026, 3, 8, 12, tzinfo=timezone(timedelta(hours=1))),
    ],
)
def test_envelope_rejects_naive_or_non_utc_timestamps(
    occurred_at: datetime,
) -> None:
    with pytest.raises(ValueError, match="UTC"):
        _envelope(occurred_at=occurred_at)


@pytest.mark.parametrize("schema_version", [None, True, False, 0, -1, 1.5, "1"])
def test_envelope_rejects_missing_or_non_positive_integer_schema_versions(
    schema_version: object,
) -> None:
    values = _envelope().to_dict()
    if schema_version is None:
        del values["schema_version"]
    else:
        values["schema_version"] = schema_version

    with pytest.raises(ValueError):
        EventEnvelope.from_dict(values)


def test_envelope_rejects_payload_identity_or_type_mismatch() -> None:
    with pytest.raises(ValueError, match="match stream_id"):
        _envelope(payload={"account_id": "account-2", "amount": 25})
    with pytest.raises(ValueError):
        _envelope(
            event_type="Withdrawn", payload={"account_id": "account-1", "amount": True}
        )
    with pytest.raises(ValueError, match="exactly"):
        _envelope(payload={"account_id": "account-1", "amount": 25, "extra": 1})


def test_accepted_result_round_trip_preserves_http_and_identity_metadata() -> None:
    result = _result()

    restored = CommandResult.from_json(result.to_json())

    assert restored == result
    assert restored.accepted
    assert not restored.rejected
    assert restored.http_status == 201
    assert restored.error_code is None
    assert restored.request_hash == REQUEST_HASH
    assert restored.created_at == OCCURRED_AT


@pytest.mark.parametrize(
    ("outcome", "error_code", "http_status", "current_version"),
    [
        (CommandOutcome.VERSION_CONFLICT, "version_conflict", 409, 3),
        (CommandOutcome.INSUFFICIENT_FUNDS, "insufficient_funds", 422, 1),
        (CommandOutcome.DOMAIN_REJECTED, "invalid_amount", 400, 0),
    ],
)
def test_rejected_results_round_trip_with_persisted_error_metadata(
    outcome: CommandOutcome,
    error_code: str,
    http_status: int,
    current_version: int,
) -> None:
    result = _result(
        outcome=outcome,
        current_version=current_version,
        committed_version=None,
        event_id=None,
        error_code=error_code,
        http_status=http_status,
    )

    restored = CommandResult.from_dict(result.to_dict())

    assert restored == result
    assert restored.rejected
    assert not restored.accepted
    assert restored.error_code == error_code
    assert restored.http_status == http_status


def test_command_result_is_immutable_and_serializes_deterministically() -> None:
    result = _result()

    assert result.to_json() == result.to_json()
    assert list(json.loads(result.to_json())) == sorted(json.loads(result.to_json()))
    with pytest.raises(FrozenInstanceError):
        result.http_status = 200  # type: ignore[misc]


@pytest.mark.parametrize(
    "overrides",
    [
        {"request_hash": b"too-short"},
        {"expected_version": True},
        {"current_version": -1},
        {"created_at": datetime(2026, 3, 8, 12)},
        {"http_status": True},
        {"outcome": "unknown"},
    ],
)
def test_command_result_rejects_invalid_persisted_values(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        _result(**overrides)


def test_accepted_and_rejected_result_invariants_are_enforced() -> None:
    with pytest.raises(ValueError, match="requires committed_version and event_id"):
        _result(committed_version=None)
    with pytest.raises(ValueError, match="cannot contain error_code"):
        _result(error_code="unexpected")
    with pytest.raises(ValueError, match="successful HTTP status"):
        _result(http_status=409)
    with pytest.raises(
        ValueError, match="cannot contain committed_version or event_id"
    ):
        _result(
            outcome=CommandOutcome.VERSION_CONFLICT,
            error_code="version_conflict",
            http_status=409,
        )
    with pytest.raises(ValueError, match="requires a stable error_code"):
        _result(
            outcome=CommandOutcome.DOMAIN_REJECTED,
            committed_version=None,
            event_id=None,
            error_code=None,
            http_status=400,
        )


def test_result_json_rejects_invalid_hash_and_non_utc_timestamp() -> None:
    values = _result().to_dict()
    values["request_hash"] = "not-hex"
    with pytest.raises(ValueError, match="hexadecimal"):
        CommandResult.from_dict(values)

    values = _result().to_dict()
    values["created_at"] = "2026-03-08T12:34:56.123456+01:00"
    with pytest.raises(ValueError, match="UTC"):
        CommandResult.from_dict(values)
