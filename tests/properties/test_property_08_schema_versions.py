"""Feature: cloudscale-production-readiness, Property 8.

Property 8: Schema versions are positive integers.
Validates: Requirements 3.9, 10.1.
"""

from copy import deepcopy
from datetime import UTC, datetime
from typing import TypeAlias
from uuid import UUID

import pytest
from hypothesis import given, settings, strategies as st

from cloudscale.domain.commands import MAX_SIGNED_BIGINT
from cloudscale.domain.events import EventEnvelope

SchemaVersionCase: TypeAlias = tuple[str, object, bool]


@st.composite
def _schema_version_cases(draw: st.DrawFn) -> SchemaVersionCase:
    category = draw(
        st.sampled_from(
            (
                "missing",
                "boolean",
                "non_integer",
                "zero",
                "negative",
                "oversized",
                "valid",
            )
        )
    )
    if category == "missing":
        return category, None, False
    if category == "boolean":
        return category, draw(st.booleans()), False
    if category == "non_integer":
        value = draw(
            st.one_of(
                st.none(),
                st.floats(allow_nan=False, allow_infinity=False),
                st.text(max_size=32),
                st.binary(max_size=32),
                st.lists(st.integers(), max_size=4),
                st.dictionaries(st.text(max_size=8), st.integers(), max_size=4),
            )
        )
        return category, value, False
    if category == "zero":
        return category, 0, False
    if category == "negative":
        value = draw(st.integers(min_value=-2 * MAX_SIGNED_BIGINT, max_value=-1))
        return category, value, False
    if category == "oversized":
        value = draw(
            st.integers(
                min_value=MAX_SIGNED_BIGINT + 1,
                max_value=2 * MAX_SIGNED_BIGINT,
            )
        )
        return category, value, False
    value = draw(st.integers(min_value=1, max_value=MAX_SIGNED_BIGINT))
    return category, value, True


def _wire_envelope() -> dict[str, object]:
    return {
        "event_id": str(UUID("00000000-0000-4000-8000-000000000001")),
        "stream_id": "account-1",
        "stream_version": 1,
        "event_type": "Deposited",
        "occurred_at": datetime(2026, 3, 8, 12, 34, 56, tzinfo=UTC).isoformat(),
        "correlation_id": str(UUID("00000000-0000-4000-8000-000000000002")),
        "causation_id": str(UUID("00000000-0000-4000-8000-000000000003")),
        "command_id": str(UUID("00000000-0000-4000-8000-000000000003")),
        "schema_version": 1,
        "payload": {"account_id": "account-1", "amount": 25},
    }


@settings(max_examples=100)
@given(case=_schema_version_cases())
def test_schema_versions_are_accepted_only_within_positive_bigint_range(
    case: SchemaVersionCase,
) -> None:
    """Feature: cloudscale-production-readiness, Property 8.

    Property 8: Schema versions are positive integers.
    Validates: Requirements 3.9, 10.1.
    """
    category, schema_version, should_accept = case
    wire_envelope = _wire_envelope()
    if category == "missing":
        del wire_envelope["schema_version"]
    else:
        wire_envelope["schema_version"] = schema_version
    before_validation = deepcopy(wire_envelope)

    if should_accept:
        envelope = EventEnvelope.from_dict(wire_envelope)

        assert isinstance(envelope.schema_version, int)
        assert not isinstance(envelope.schema_version, bool)
        assert 1 <= envelope.schema_version <= MAX_SIGNED_BIGINT
        assert envelope.schema_version == schema_version
    else:
        with pytest.raises(ValueError):
            EventEnvelope.from_dict(wire_envelope)

    assert wire_envelope == before_validation
