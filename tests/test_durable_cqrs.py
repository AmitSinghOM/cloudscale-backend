"""Phase 1 tests: durable event log + idempotent projection consumer.

Proves the three properties the tier is supposed to guarantee:
  - durability   : events + read model survive a simulated process restart
  - idempotency  : duplicate / replayed delivery has exactly-once effect
  - ordering     : per-stream seq and global log id are monotonic and correct

Runnable via `python -m pytest -q` or, without pytest,
`python -m tests.test_durable_cqrs`.
"""

import os
import tempfile

from cqrs import (
    BalanceProjection,
    CommandHandler,
    ConcurrencyError,
    IdempotentProjectionStore,
    SqliteEventStore,
    run_consumer,
)


# --------------------------------------------------------------------------
# Durability
# --------------------------------------------------------------------------


def test_events_survive_a_simulated_restart():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "events.db")

        # "Process 1" writes, then closes (simulated crash/shutdown).
        store = SqliteEventStore(path)
        handler = CommandHandler(store)
        handler.handle({"type": "Deposit", "account_id": "a1", "amount": 100})
        handler.handle({"type": "Withdraw", "account_id": "a1", "amount": 30})
        store.close()

        # "Process 2" reopens the same file and sees everything.
        store2 = SqliteEventStore(path)
        events = store2.read("account-a1")
        assert [e["type"] for e in events] == ["Deposited", "Withdrawn"]
        assert BalanceProjection().rebuild(events)["balance"] == 70
        store2.close()


def test_projection_read_model_survives_restart():
    with tempfile.TemporaryDirectory() as d:
        log_path = os.path.join(d, "events.db")
        proj_path = os.path.join(d, "read.db")

        store = SqliteEventStore(log_path)
        handler = CommandHandler(store)
        handler.handle({"type": "Deposit", "account_id": "a1", "amount": 100})
        handler.handle({"type": "Deposit", "account_id": "a1", "amount": 25})

        proj = IdempotentProjectionStore(proj_path)
        run_consumer(store, proj)
        assert proj.balance("a1")["balance"] == 125
        last = proj.last_id()
        proj.close()
        store.close()

        # Reopen the read model: balance and offset are still there.
        proj2 = IdempotentProjectionStore(proj_path)
        assert proj2.balance("a1")["balance"] == 125
        assert proj2.last_id() == last
        proj2.close()


# --------------------------------------------------------------------------
# Idempotency (at-least-once delivery -> exactly-once effect)
# --------------------------------------------------------------------------


def test_duplicate_delivery_does_not_double_count():
    store = SqliteEventStore(":memory:")
    handler = CommandHandler(store)
    handler.handle({"type": "Deposit", "account_id": "a1", "amount": 100})

    proj = IdempotentProjectionStore(":memory:")
    events = store.read_all()

    # Deliver every event twice, out of a naive "at-least-once" feed.
    first = [proj.apply(e) for e in events]
    second = [proj.apply(e) for e in events]

    assert all(first), "first delivery should mutate state"
    assert not any(second), "second delivery must be skipped as duplicate"
    assert proj.balance("a1")["balance"] == 100
    assert proj.balance("a1")["version"] == 1


def test_replay_from_zero_is_idempotent():
    store = SqliteEventStore(":memory:")
    handler = CommandHandler(store)
    handler.handle({"type": "Deposit", "account_id": "a1", "amount": 40})
    handler.handle({"type": "Withdraw", "account_id": "a1", "amount": 10})

    proj = IdempotentProjectionStore(":memory:")
    run_consumer(store, proj)
    balance_after_first = proj.balance("a1")["balance"]

    # Full replay of the whole log (as a recovering consumer would do) must
    # not change the read model.
    for e in store.read_all():
        proj.apply(e)
    assert proj.balance("a1")["balance"] == balance_after_first == 30


def test_crash_between_apply_and_offset_commit_is_safe():
    # run_consumer resumes from the persisted offset; re-running it after a
    # "crash" re-delivers overlapping events, which idempotent apply absorbs.
    store = SqliteEventStore(":memory:")
    handler = CommandHandler(store)
    handler.handle({"type": "Deposit", "account_id": "a1", "amount": 100})

    proj = IdempotentProjectionStore(":memory:")
    run_consumer(store, proj)  # first pass
    handler.handle({"type": "Deposit", "account_id": "a1", "amount": 5})
    run_consumer(store, proj)  # resume: only the new event mutates
    run_consumer(store, proj)  # re-run over the same log: no effect

    assert proj.balance("a1")["balance"] == 105
    assert proj.balance("a1")["version"] == 2


# --------------------------------------------------------------------------
# Ordering
# --------------------------------------------------------------------------


def test_per_stream_seq_is_monotonic_and_one_based():
    store = SqliteEventStore(":memory:")
    handler = CommandHandler(store)
    s1 = handler.handle({"type": "Deposit", "account_id": "a1", "amount": 10})
    s2 = handler.handle({"type": "Deposit", "account_id": "a1", "amount": 10})
    s3 = handler.handle({"type": "Deposit", "account_id": "a1", "amount": 10})
    assert [s1, s2, s3] == [1, 2, 3]
    assert [e["seq"] for e in store.read("account-a1")] == [1, 2, 3]


def test_streams_have_independent_sequences():
    store = SqliteEventStore(":memory:")
    handler = CommandHandler(store)
    handler.handle({"type": "Deposit", "account_id": "a1", "amount": 1})
    handler.handle({"type": "Deposit", "account_id": "a2", "amount": 1})
    handler.handle({"type": "Deposit", "account_id": "a1", "amount": 1})
    assert [e["seq"] for e in store.read("account-a1")] == [1, 2]
    assert [e["seq"] for e in store.read("account-a2")] == [1]


def test_global_log_id_gives_total_order():
    store = SqliteEventStore(":memory:")
    handler = CommandHandler(store)
    handler.handle({"type": "Deposit", "account_id": "a1", "amount": 1})
    handler.handle({"type": "Deposit", "account_id": "a2", "amount": 1})
    handler.handle({"type": "Deposit", "account_id": "a1", "amount": 1})

    feed = store.read_all()
    ids = [e["id"] for e in feed]
    assert ids == sorted(ids), "global feed must be totally ordered by id"
    assert len(ids) == len(set(ids)) == 3
    # Interleaved streams preserve their relative per-stream order in the feed.
    a1_seqs = [e["seq"] for e in feed if e["account_id"] == "a1"]
    assert a1_seqs == [1, 2]


def test_read_all_after_id_pages_forward():
    store = SqliteEventStore(":memory:")
    handler = CommandHandler(store)
    for _ in range(5):
        handler.handle({"type": "Deposit", "account_id": "a1", "amount": 1})

    first_two = store.read_all(after_id=0, limit=2)
    assert [e["id"] for e in first_two] == [1, 2]
    next_batch = store.read_all(after_id=first_two[-1]["id"])
    assert [e["id"] for e in next_batch] == [3, 4, 5]


# --------------------------------------------------------------------------
# Seam: the command side is agnostic to which store it appends to
# --------------------------------------------------------------------------


def test_command_handler_works_against_durable_store():
    store = SqliteEventStore(":memory:")
    handler = CommandHandler(store)
    handler.handle({"type": "Deposit", "account_id": "a1", "amount": 50})

    # No-overdraft rule still enforced by replay against the durable log.
    raised = False
    try:
        handler.handle({"type": "Withdraw", "account_id": "a1", "amount": 999})
    except Exception as exc:  # CommandError
        raised = "insufficient funds" in str(exc)
    assert raised
    assert len(store.read("account-a1")) == 1


def test_duplicate_event_id_append_is_rejected():
    store = SqliteEventStore(":memory:")
    store.append(
        "account-a1",
        {"event_id": "fixed", "type": "Deposited", "account_id": "a1", "amount": 1},
    )
    raised = False
    try:
        store.append(
            "account-a1",
            {"event_id": "fixed", "type": "Deposited", "account_id": "a1", "amount": 1},
        )
    except ConcurrencyError:
        raised = True
    assert raised


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
