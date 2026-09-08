"""Feature: cloudscale-production-readiness, Property 21.

Property 21: SQLite compatibility is replayable and idempotent.
Validates: Requirements 1.8.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from string import ascii_lowercase, digits
from tempfile import TemporaryDirectory

from hypothesis import given, settings
from hypothesis import strategies as st

import cqrs
from cloudscale.adapters.compat import (
    domain_event_to_legacy,
    legacy_event_to_domain,
)
from cloudscale.domain.account import fold

_ACCOUNT_IDS = st.text(
    alphabet=ascii_lowercase + digits + "-_",
    min_size=1,
    max_size=12,
)


@dataclass(frozen=True, slots=True)
class _CompatibilityCase:
    events: tuple[dict[str, object], ...]
    duplicate_counts: tuple[int, ...]
    restart_after: int


@st.composite
def _compatibility_cases(draw: st.DrawFn) -> _CompatibilityCase:
    account_ids = draw(st.lists(_ACCOUNT_IDS, min_size=1, max_size=3, unique=True))
    raw_events = draw(
        st.lists(
            st.tuples(
                st.integers(min_value=0, max_value=len(account_ids) - 1),
                st.booleans(),
                st.integers(min_value=1, max_value=100),
            ),
            min_size=1,
            max_size=12,
        )
    )
    event_ids = draw(
        st.lists(
            st.uuids(),
            min_size=len(raw_events),
            max_size=len(raw_events),
            unique=True,
        )
    )

    balances: defaultdict[str, int] = defaultdict(int)
    events: list[dict[str, object]] = []
    for event_id, (account_index, request_withdrawal, generated_amount) in zip(
        event_ids, raw_events, strict=True
    ):
        account_id = account_ids[account_index]
        if request_withdrawal and balances[account_id] > 0:
            amount = min(generated_amount, balances[account_id])
            event_type = "Withdrawn"
            balances[account_id] -= amount
        else:
            amount = generated_amount
            event_type = "Deposited"
            balances[account_id] += amount
        events.append(
            {
                "event_id": str(event_id),
                "type": event_type,
                "account_id": account_id,
                "amount": amount,
            }
        )

    duplicate_counts = draw(
        st.lists(
            st.integers(min_value=1, max_value=3),
            min_size=len(events),
            max_size=len(events),
        )
    )
    duplicated_index = draw(st.integers(min_value=0, max_value=len(events) - 1))
    duplicate_counts[duplicated_index] = max(2, duplicate_counts[duplicated_index])
    delivery_count = sum(duplicate_counts)

    return _CompatibilityCase(
        events=tuple(events),
        duplicate_counts=tuple(duplicate_counts),
        restart_after=draw(st.integers(min_value=0, max_value=delivery_count)),
    )


def _without_global_id(event: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in event.items() if key != "id"}


def _expected_replays(
    feed: list[dict[str, object]],
) -> dict[str, dict[str, object]]:
    expected: dict[str, dict[str, object]] = {}
    account_ids = {str(event["account_id"]) for event in feed}
    for account_id in account_ids:
        stream = [event for event in feed if event["account_id"] == account_id]
        legacy_state = cqrs.BalanceProjection().rebuild(stream)
        typed_events = [legacy_event_to_domain(event) for event in stream]
        typed_state = fold(typed_events)

        assert [
            domain_event_to_legacy(typed, metadata=legacy)
            for typed, legacy in zip(typed_events, stream, strict=True)
        ] == stream
        assert legacy_state == {
            "account_id": typed_state.account_id,
            "balance": typed_state.balance,
            "version": typed_state.version,
        }
        expected[account_id] = legacy_state
    return expected


def _assert_projection_states(
    projection: cqrs.IdempotentProjectionStore,
    expected: dict[str, dict[str, object]],
) -> None:
    for account_id, state in expected.items():
        assert projection.balance(account_id) == state


@settings(max_examples=100, deadline=None)
@given(case=_compatibility_cases())
def test_sqlite_compatibility_is_replayable_and_idempotent(
    case: _CompatibilityCase,
) -> None:
    """Feature: cloudscale-production-readiness, Property 21: SQLite compatibility is replayable and idempotent."""
    with TemporaryDirectory(prefix="cloudscale-property-21-") as directory:
        event_path = Path(directory) / "events.sqlite3"
        projection_path = Path(directory) / "projection.sqlite3"

        event_store = cqrs.SqliteEventStore(str(event_path))
        try:
            stream_versions: defaultdict[str, int] = defaultdict(int)
            for event in case.events:
                account_id = str(event["account_id"])
                stream_versions[account_id] += 1
                assert (
                    event_store.append(f"account-{account_id}", event)
                    == stream_versions[account_id]
                )
        finally:
            event_store.close()

        event_store = cqrs.SqliteEventStore(str(event_path))
        try:
            feed = event_store.read_all()
            assert [event["id"] for event in feed] == list(
                range(1, len(case.events) + 1)
            )
            assert [event["event_id"] for event in feed] == [
                event["event_id"] for event in case.events
            ]

            for account_id, event_count in stream_versions.items():
                stream = event_store.read(f"account-{account_id}")
                assert [event["seq"] for event in stream] == list(
                    range(1, event_count + 1)
                )
                assert stream == [
                    _without_global_id(event)
                    for event in feed
                    if event["account_id"] == account_id
                ]

            expected = _expected_replays(feed)
            deliveries = [
                event
                for event, duplicate_count in zip(
                    feed, case.duplicate_counts, strict=True
                )
                for _ in range(duplicate_count)
            ]
            assert len(deliveries) > len(feed)

            projection = cqrs.IdempotentProjectionStore(str(projection_path))
            applied_by_event_id: Counter[str] = Counter()
            checkpoints = [projection.last_id()]
            try:
                for event in deliveries[: case.restart_after]:
                    applied_by_event_id[str(event["event_id"])] += projection.apply(
                        event
                    )
                    checkpoints.append(projection.last_id())

                persisted_checkpoint = projection.last_id()
                persisted_states = {
                    account_id: projection.balance(account_id)
                    for account_id in expected
                }
            finally:
                projection.close()
        finally:
            event_store.close()

        event_store = cqrs.SqliteEventStore(str(event_path))
        projection = cqrs.IdempotentProjectionStore(str(projection_path))
        try:
            assert event_store.read_all() == feed
            assert projection.last_id() == persisted_checkpoint
            checkpoints.append(projection.last_id())
            for account_id, state in persisted_states.items():
                assert projection.balance(account_id) == state

            for event in deliveries[case.restart_after :]:
                applied_by_event_id[str(event["event_id"])] += projection.apply(event)
                checkpoints.append(projection.last_id())

            assert applied_by_event_id == Counter(
                {str(event["event_id"]): 1 for event in feed}
            )
            assert checkpoints == sorted(checkpoints)
            assert projection.last_id() == int(feed[-1]["id"])
            _assert_projection_states(projection, expected)

            before_replay = {
                account_id: projection.balance(account_id) for account_id in expected
            }
            checkpoint_before_replay = projection.last_id()
            assert all(projection.apply(event) is False for event in feed)
            assert cqrs.run_consumer(event_store, projection) == 0
            assert projection.last_id() == checkpoint_before_replay
            assert {
                account_id: projection.balance(account_id) for account_id in expected
            } == before_replay
        finally:
            projection.close()
            event_store.close()
