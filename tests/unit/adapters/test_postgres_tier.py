"""Postgres tier guarantees, verified against a live local PostgreSQL.

Skipped entirely when no server is reachable at ``CLOUDSCALE_TEST_PG``
(default ``postgresql://localhost/postgres``). A throwaway database is
created per test session and DROPPED in teardown — nothing is left behind.

These are the same guarantees the SQLite tier proved: optimistic append,
per-stream and global ordering, durability across reconnect, exactly-once
apply under duplicate/replay delivery, dead-lettering that never wedges the
log, redrive, and the resilient consumer end-to-end.
"""

from __future__ import annotations

import os
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from cloudscale.adapters.postgres.event_store import PostgresEventStore  # noqa: E402
from cloudscale.adapters.postgres.projection_store import (  # noqa: E402
    PostgresProjectionStore,
)
from cloudscale.adapters.sqlite_compat.dead_letter_store import (  # noqa: E402
    RedriveOutcome,
)
from cloudscale.processes.resilient_consumer import ResilientConsumer  # noqa: E402
from cloudscale.resilience import RetryPolicy  # noqa: E402
from cqrs import ConcurrencyError  # noqa: E402

_ADMIN_DSN = os.environ.get("CLOUDSCALE_TEST_PG", "postgresql://localhost/postgres")


def _postgres_available() -> bool:
    try:
        with psycopg.connect(_ADMIN_DSN, connect_timeout=2):
            return True
    except psycopg.OperationalError:
        return False


pytestmark = pytest.mark.skipif(
    not _postgres_available(),
    reason=f"no PostgreSQL reachable at {_ADMIN_DSN}",
)


@pytest.fixture(scope="module")
def throwaway_dsn():
    """Create a uniquely named database and DROP it after the module."""
    database = f"cloudscale_test_{uuid.uuid4().hex[:12]}"
    admin = psycopg.connect(_ADMIN_DSN, autocommit=True)
    admin.execute(f'CREATE DATABASE "{database}"')
    base = _ADMIN_DSN.rsplit("/", 1)[0]
    try:
        yield f"{base}/{database}"
    finally:
        admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
        admin.close()


@pytest.fixture()
def stream() -> str:
    """Unique stream/account per test so tests share the database safely."""
    return f"acct-{uuid.uuid4().hex[:10]}"


@pytest.fixture()
def consumer_name() -> str:
    return f"balances-{uuid.uuid4().hex[:10]}"


def _deposit(account: str, amount: int, event_id: str | None = None) -> dict:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "type": "Deposited",
        "account_id": account,
        "amount": amount,
    }


def test_append_orders_streams_and_rejects_duplicate_event_ids(
    throwaway_dsn: str, stream: str
) -> None:
    store = PostgresEventStore(throwaway_dsn)
    try:
        assert store.append(stream, _deposit(stream, 10)) == 1
        assert store.append(stream, _deposit(stream, 20)) == 2
        events = store.read(stream)
        assert [event["seq"] for event in events] == [1, 2]
        assert [event["amount"] for event in events] == [10, 20]
        assert store.read_after(stream, 1)[0]["amount"] == 20

        duplicated = _deposit(stream, 5, event_id=events[0]["event_id"])
        with pytest.raises(ConcurrencyError):
            store.append(stream, duplicated)
    finally:
        store.close()


def test_durability_across_reconnect(throwaway_dsn: str, stream: str) -> None:
    first = PostgresEventStore(throwaway_dsn)
    first.append(stream, _deposit(stream, 42))
    first.close()

    second = PostgresEventStore(throwaway_dsn)
    try:
        events = second.read(stream)
        assert len(events) == 1 and events[0]["amount"] == 42
    finally:
        second.close()


def test_apply_is_exactly_once_under_duplicate_delivery(
    throwaway_dsn: str, stream: str, consumer_name: str
) -> None:
    projection = PostgresProjectionStore(throwaway_dsn, consumer=consumer_name)
    try:
        event = _deposit(stream, 30)
        event["id"] = 7
        assert projection.apply(dict(event)) is True
        assert projection.apply(dict(event)) is False  # replayed duplicate
        assert projection.balance(stream)["balance"] == 30
        assert projection.balance(stream)["version"] == 1
        assert projection.last_id() == 7
    finally:
        projection.close()


def test_offset_survives_reconnect(
    throwaway_dsn: str, stream: str, consumer_name: str
) -> None:
    first = PostgresProjectionStore(throwaway_dsn, consumer=consumer_name)
    event = _deposit(stream, 5)
    event["id"] = 3
    first.apply(event)
    first.close()

    second = PostgresProjectionStore(throwaway_dsn, consumer=consumer_name)
    try:
        assert second.last_id() == 3
        assert second.balance(stream)["balance"] == 5
    finally:
        second.close()


def test_dead_letter_and_redrive_roundtrip(
    throwaway_dsn: str, stream: str, consumer_name: str
) -> None:
    projection = PostgresProjectionStore(throwaway_dsn, consumer=consumer_name)
    try:
        good = _deposit(stream, 25)
        good["id"] = 1
        assert projection.dead_letter(good, ValueError("transient-looking"), 3)
        assert projection.dead_letter(good, ValueError("replayed"), 1) is False
        assert projection.last_id() == 1
        assert projection.dead_letter_count() == 1

        # Redrive applies it and removes the letter; replays still dedupe.
        assert projection.redrive(good["event_id"]) is RedriveOutcome.APPLIED
        assert projection.dead_letter_count() == 0
        assert projection.balance(stream)["balance"] == 25
        assert projection.apply(dict(good)) is False

        # A genuinely poisonous payload re-parks with attempts + 1.
        bad = {
            "event_id": str(uuid.uuid4()),
            "id": 2,
            "type": "Deposited",
            "account_id": stream,
            "amount": None,
        }
        assert projection.dead_letter(bad, ValueError("bad amount"), 3)
        assert projection.redrive(bad["event_id"]) is RedriveOutcome.FAILED_AGAIN
        assert projection.dead_letters()[0]["attempts"] == 4
        assert projection.redrive("missing") is RedriveOutcome.NOT_FOUND
    finally:
        projection.close()


def test_writes_are_visible_to_a_second_connection_after_reads(
    throwaway_dsn: str, stream: str, consumer_name: str
) -> None:
    """Regression lock for the psycopg3 savepoint trap.

    A bare read before ``conn.transaction()`` on a non-autocommit connection
    silently downgrades the block to a savepoint that never commits — the
    writer's own connection sees the data, every other connection never does.
    This test performs a read FIRST (the consumer loop's exact sequence:
    ``last_id()`` then ``apply()``), then asserts the write through a
    SEPARATE connection while the writer is still open.
    """
    writer = PostgresProjectionStore(throwaway_dsn, consumer=consumer_name)
    observer = PostgresProjectionStore(throwaway_dsn, consumer=consumer_name)
    try:
        assert writer.last_id() == 0  # bare read first — arms the trap
        event = _deposit(stream, 55)
        event["id"] = 9
        assert writer.apply(event) is True

        # The OTHER connection must see it immediately, writer still open.
        assert observer.balance(stream)["balance"] == 55
        assert observer.last_id() == 9

        # Same discipline on the event store: read first, then append.
        store_writer = PostgresEventStore(throwaway_dsn)
        store_observer = PostgresEventStore(throwaway_dsn)
        try:
            assert store_writer.read(stream) == []  # bare read first
            store_writer.append(stream, _deposit(stream, 1))
            assert len(store_observer.read(stream)) == 1
        finally:
            store_writer.close()
            store_observer.close()
    finally:
        writer.close()
        observer.close()


def test_account_registry_semantics_and_cross_connection_race(
    throwaway_dsn: str,
) -> None:
    from cloudscale.adapters.postgres.account_registry import PostgresAccountRegistry
    from cloudscale.application.ports import RegistrationOutcome

    account = f"acct-{uuid.uuid4().hex[:10]}"
    first = PostgresAccountRegistry(throwaway_dsn)
    second = PostgresAccountRegistry(throwaway_dsn)  # separate connection
    try:
        assert first.register(account, "alice") is RegistrationOutcome.CREATED
        # Visible to the other connection immediately (autocommit discipline).
        assert second.owner_of(account) == "alice"
        assert (
            second.register(account, "alice")
            is RegistrationOutcome.ALREADY_OWNED_BY_CALLER
        )
        assert second.register(account, "bob") is RegistrationOutcome.TAKEN

        # Two connections race a fresh id: exactly one CREATED, the other TAKEN.
        import threading

        raced = f"acct-{uuid.uuid4().hex[:10]}"
        barrier = threading.Barrier(2)
        outcomes: list[RegistrationOutcome] = []
        lock = threading.Lock()

        def contend(registry: PostgresAccountRegistry, subject: str) -> None:
            barrier.wait()
            outcome = registry.register(raced, subject)
            with lock:
                outcomes.append(outcome)

        threads = [
            threading.Thread(target=contend, args=(first, "alice")),
            threading.Thread(target=contend, args=(second, "bob")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert sorted(o.value for o in outcomes) == ["created", "taken"]
    finally:
        first.close()
        second.close()


def test_outbox_delivers_late_committing_smaller_ids(throwaway_dsn: str) -> None:
    """The multi-writer skew the outbox exists to fix.

    Writer A takes an IDENTITY id but has not committed; writer B takes the
    next id and commits. A consumer reading by raw id would record an offset
    past A's id and skip A forever once it commits. By outbox position, B is
    published first, then A - gapless, commit-ordered, nothing lost.
    """
    store = PostgresEventStore(throwaway_dsn)
    slow = psycopg.connect(throwaway_dsn)  # non-autocommit: holds a txn open
    account_a = f"acct-{uuid.uuid4().hex[:8]}"
    account_b = f"acct-{uuid.uuid4().hex[:8]}"
    try:
        # Drain anything pending from other tests so positions are predictable.
        head = store.read_all(0)
        after = head[-1]["id"] if head else 0

        # A: insert (id assigned) but do NOT commit yet.
        a_event_id = str(uuid.uuid4())
        slow.execute(
            "INSERT INTO events (event_id, stream, seq, type, account_id, amount) "
            "VALUES (%s, %s, 1, 'Deposited', %s, 10)",
            (a_event_id, account_a, account_a),
        )
        # B: append and commit normally -> gets the LARGER id.
        store.append(account_b, _deposit(account_b, 20))

        first = store.read_all(after)
        assert [e["account_id"] for e in first] == [account_b]
        assert first[0]["id"] == after + 1

        slow.commit()  # A becomes visible AFTER B was consumed

        second = store.read_all(first[-1]["id"])
        assert [e["event_id"] for e in second] == [a_event_id]
        assert second[0]["id"] == after + 2  # gapless, not skipped
    finally:
        slow.close()
        store.close()


def test_pooled_adapters_serve_concurrent_callers(
    throwaway_dsn: str, consumer_name: str
) -> None:
    """Multiple threads hit one pooled projection store at once; no lock, no loss."""
    import threading

    projection = PostgresProjectionStore(throwaway_dsn, consumer=consumer_name)
    account = f"acct-{uuid.uuid4().hex[:8]}"
    events = [dict(_deposit(account, 1), id=i + 1) for i in range(40)]
    try:

        def worker(chunk: list[dict]) -> None:
            for event in chunk:
                projection.apply(event)

        threads = [
            threading.Thread(target=worker, args=(events[i::4],)) for i in range(4)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert projection.balance(account) == {
            "account_id": account,
            "balance": 40,
            "version": 40,
        }
    finally:
        projection.close()


def test_resilient_consumer_end_to_end_on_postgres(
    throwaway_dsn: str, consumer_name: str
) -> None:
    store = PostgresEventStore(throwaway_dsn)
    projection = PostgresProjectionStore(throwaway_dsn, consumer=consumer_name)
    account = f"acct-{uuid.uuid4().hex[:10]}"
    try:
        offset_before = projection.last_id()
        for amount in (10, 20, 30):
            store.append(account, _deposit(account, amount))
        # One poison event: fails typed validation at apply time.
        store.append(
            account, {"type": "Deposited", "account_id": account, "amount": None}
        )
        store.append(account, _deposit(account, 40))

        consumer = ResilientConsumer(
            store,
            projection,
            retry_policy=RetryPolicy(max_attempts=2, base_delay_seconds=0.0),
            retryable_errors=(psycopg.OperationalError,),
        )
        report = consumer.run()

        assert report.applied >= 4
        assert report.dead_lettered == 1
        assert report.halted is False
        assert projection.balance(account)["balance"] == 100
        assert projection.last_id() > offset_before
        # Re-running the consumer is a no-op: offset persisted, dedupe holds.
        assert (
            ResilientConsumer(
                store, projection, retryable_errors=(psycopg.OperationalError,)
            )
            .run()
            .applied
            == 0
        )
        assert projection.balance(account)["balance"] == 100
    finally:
        projection.close()
        store.close()
