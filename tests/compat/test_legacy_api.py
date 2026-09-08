"""Regression coverage for the legacy dictionary compatibility boundary."""

from inspect import signature

import cqrs
from cloudscale.adapters.compat import (
    domain_event_to_legacy,
    legacy_event_to_domain,
)
from cloudscale.domain.events import Deposited, Withdrawn
from cqrs.durable_eventstore import (
    ConcurrencyError as OriginalConcurrencyError,
)
from cqrs.durable_eventstore import SqliteEventStore as OriginalSqliteEventStore
from cqrs.eventstore import EventStore as OriginalEventStore
from cqrs.idempotent_consumer import (
    IdempotentProjectionStore as OriginalProjectionStore,
)
from cqrs.idempotent_consumer import run_consumer as original_run_consumer
from cqrs.projections import BalanceProjection as OriginalBalanceProjection

EXPECTED_EXPORTS = (
    "EventStore",
    "SqliteEventStore",
    "ConcurrencyError",
    "CommandHandler",
    "CommandError",
    "BalanceProjection",
    "IdempotentProjectionStore",
    "run_consumer",
)


def test_legacy_events_round_trip_through_typed_domain_values() -> None:
    deposited = {
        "id": 7,
        "event_id": "event-7",
        "stream": "account-a1",
        "seq": 2,
        "type": "Deposited",
        "account_id": "a1",
        "amount": 25,
    }
    withdrawn = {"type": "Withdrawn", "account_id": "a1", "amount": 5}

    typed_deposit = legacy_event_to_domain(deposited)
    typed_withdrawal = legacy_event_to_domain(withdrawn)

    assert typed_deposit == Deposited(account_id="a1", amount=25)
    assert typed_withdrawal == Withdrawn(account_id="a1", amount=5)
    assert domain_event_to_legacy(typed_deposit, metadata=deposited) == deposited
    assert domain_event_to_legacy(typed_withdrawal) == withdrawn


def test_cqrs_exports_and_legacy_signatures_remain_stable() -> None:
    assert tuple(cqrs.__all__) == EXPECTED_EXPORTS
    assert cqrs.ConcurrencyError is OriginalConcurrencyError
    assert signature(cqrs.EventStore) == signature(OriginalEventStore)
    assert signature(cqrs.EventStore.append) == signature(OriginalEventStore.append)
    assert signature(cqrs.EventStore.read) == signature(OriginalEventStore.read)
    assert signature(cqrs.SqliteEventStore) == signature(OriginalSqliteEventStore)
    assert signature(cqrs.SqliteEventStore.append) == signature(
        OriginalSqliteEventStore.append
    )
    assert signature(cqrs.SqliteEventStore.read) == signature(
        OriginalSqliteEventStore.read
    )
    assert signature(cqrs.SqliteEventStore.read_all) == signature(
        OriginalSqliteEventStore.read_all
    )
    assert signature(cqrs.BalanceProjection.apply) == signature(
        OriginalBalanceProjection.apply
    )
    assert signature(cqrs.BalanceProjection.rebuild) == signature(
        OriginalBalanceProjection.rebuild
    )
    assert signature(cqrs.IdempotentProjectionStore) == signature(
        OriginalProjectionStore
    )
    assert signature(cqrs.IdempotentProjectionStore.apply) == signature(
        OriginalProjectionStore.apply
    )
    assert signature(cqrs.run_consumer) == signature(original_run_consumer)


def test_sqlite_adapters_expose_non_production_metadata() -> None:
    event_store = cqrs.SqliteEventStore(":memory:")
    projection_store = cqrs.IdempotentProjectionStore(":memory:")
    try:
        expected = {
            "environment": "non-production",
            "storage_tier": "sqlite-compatibility",
            "production": False,
        }
        assert event_store.metadata == expected
        assert event_store.startup_metadata() == expected
        assert event_store.health_metadata() == expected
        assert projection_store.metadata == expected
        assert projection_store.startup_metadata() == expected
        assert projection_store.health_metadata() == expected
    finally:
        event_store.close()
        projection_store.close()


def test_sqlite_adapters_preserve_feed_dedupe_and_checkpoint_semantics() -> None:
    event_store = cqrs.SqliteEventStore(":memory:")
    projection_store = cqrs.IdempotentProjectionStore(":memory:")
    try:
        sequence = event_store.append(
            "account-a1",
            {
                "event_id": "event-1",
                "type": "Deposited",
                "account_id": "a1",
                "amount": 40,
            },
        )

        stream = event_store.read("account-a1")
        feed = event_store.read_all()
        assert sequence == 1
        assert stream[0]["seq"] == 1
        assert feed[0]["id"] == 1
        assert feed[0]["event_id"] == "event-1"

        assert cqrs.run_consumer(event_store, projection_store) == 1
        checkpoint = projection_store.last_id()
        assert projection_store.apply(feed[0]) is False
        assert projection_store.last_id() == checkpoint == 1
        assert projection_store.balance("a1") == {
            "account_id": "a1",
            "balance": 40,
            "version": 1,
        }
    finally:
        event_store.close()
        projection_store.close()
