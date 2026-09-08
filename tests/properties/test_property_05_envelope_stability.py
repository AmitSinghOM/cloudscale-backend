"""Feature: cloudscale-production-readiness, Property 5.

Property 5: Event envelopes are complete and stable.
Validates: Requirements 3.1, 3.2, 3.3, 3.8, 18.8, 18.9.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from datetime import UTC, datetime
from uuid import UUID

from hypothesis import given, settings
from hypothesis import strategies as st

from cloudscale.domain.commands import MAX_SIGNED_BIGINT
from cloudscale.domain.events import Deposited, EventEnvelope, Withdrawn

_REQUIRED_FIELDS = {
    "event_id",
    "stream_id",
    "stream_version",
    "event_type",
    "occurred_at",
    "correlation_id",
    "causation_id",
    "command_id",
    "schema_version",
    "payload",
}
_ACCOUNT_IDS = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",)),
    min_size=1,
    max_size=64,
)
_UTC_TIMESTAMPS = st.datetimes(
    min_value=datetime(2000, 1, 1),
    max_value=datetime(2100, 12, 31, 23, 59, 59, 999999),
    timezones=st.just(UTC),
    allow_imaginary=False,
)


@st.composite
def _valid_envelopes(draw: st.DrawFn) -> EventEnvelope:
    account_id = draw(_ACCOUNT_IDS)
    amount = draw(st.integers(min_value=1, max_value=MAX_SIGNED_BIGINT))
    event = (
        Deposited(account_id, amount)
        if draw(st.booleans())
        else Withdrawn(account_id, amount)
    )
    event_id, command_id, correlation_id = draw(
        st.lists(st.uuids(version=4), min_size=3, max_size=3, unique=True)
    )

    return EventEnvelope.from_domain_event(
        event,
        event_id=event_id,
        stream_version=draw(st.integers(min_value=1, max_value=MAX_SIGNED_BIGINT)),
        occurred_at=draw(_UTC_TIMESTAMPS),
        correlation_id=correlation_id,
        causation_id=command_id,
        command_id=command_id,
        schema_version=1,
    )


def _protected_identity(envelope: EventEnvelope) -> tuple[object, ...]:
    return (
        envelope.event_id,
        envelope.stream_id,
        envelope.stream_version,
        envelope.command_id,
        envelope.correlation_id,
        envelope.causation_id,
        envelope.occurred_at,
    )


def _assert_complete_typed_envelope(envelope: EventEnvelope) -> None:
    assert isinstance(envelope.event_id, UUID)
    assert isinstance(envelope.stream_id, str) and envelope.stream_id
    assert isinstance(envelope.stream_version, int)
    assert not isinstance(envelope.stream_version, bool)
    assert envelope.stream_version >= 1
    assert envelope.event_type in ("Deposited", "Withdrawn")
    assert isinstance(envelope.occurred_at, datetime)
    assert envelope.occurred_at.tzinfo is not None
    assert envelope.occurred_at.utcoffset() is not None
    assert envelope.occurred_at.utcoffset().total_seconds() == 0
    assert isinstance(envelope.correlation_id, UUID)
    assert isinstance(envelope.causation_id, UUID)
    assert isinstance(envelope.command_id, UUID)
    assert envelope.causation_id == envelope.command_id
    assert isinstance(envelope.schema_version, int)
    assert not isinstance(envelope.schema_version, bool)
    assert envelope.schema_version >= 1
    assert isinstance(envelope.payload, Mapping)
    assert set(envelope.payload) == {"account_id", "amount"}
    assert envelope.payload["account_id"] == envelope.stream_id


@settings(max_examples=100)
@given(envelope=_valid_envelopes())
def test_event_envelopes_are_complete_and_stable(envelope: EventEnvelope) -> None:
    """Property 5: required types and protected identity survive every v1 seam."""
    _assert_complete_typed_envelope(envelope)
    original_identity = _protected_identity(envelope)

    persisted_dict = envelope.to_dict()
    persisted_dict_before = deepcopy(persisted_dict)
    assert set(persisted_dict) == _REQUIRED_FIELDS

    from_dict = EventEnvelope.from_dict(persisted_dict)
    assert persisted_dict == persisted_dict_before

    persisted_json = from_dict.to_json()
    from_json = EventEnvelope.from_json(persisted_json)

    for restored in (from_dict, from_json):
        _assert_complete_typed_envelope(restored)
        assert restored == envelope
        assert _protected_identity(restored) == original_identity

    assert from_json.to_dict() == persisted_dict_before
    assert from_json.to_json() == persisted_json
