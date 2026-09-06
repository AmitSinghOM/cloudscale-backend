"""Phase 0 CQRS tests: writes append events, reads serve projections.

Runnable via `python -m pytest -q` or, without pytest, `python -m tests.test_cqrs`.
"""

from cqrs import BalanceProjection, CommandError, CommandHandler, EventStore


def test_command_appends_event():
    store = EventStore()
    handler = CommandHandler(store)

    seq = handler.handle({"type": "Deposit", "account_id": "a1", "amount": 100})

    assert seq == 1
    events = store.read("account-a1")
    assert len(events) == 1
    assert events[0]["type"] == "Deposited"
    assert events[0]["amount"] == 100
    assert events[0]["seq"] == 1


def test_projection_rebuilds_state_from_events():
    events = [
        {"type": "Deposited", "account_id": "a1", "amount": 100},
        {"type": "Withdrawn", "account_id": "a1", "amount": 30},
        {"type": "Deposited", "account_id": "a1", "amount": 5},
    ]

    read_model = BalanceProjection().rebuild(events)

    assert read_model["account_id"] == "a1"
    assert read_model["balance"] == 75
    assert read_model["version"] == 3


def test_read_reflects_writes():
    store = EventStore()
    handler = CommandHandler(store)
    projection = BalanceProjection()

    handler.handle({"type": "Deposit", "account_id": "a1", "amount": 100})
    handler.handle({"type": "Withdraw", "account_id": "a1", "amount": 40})

    read_model = projection.rebuild(store.read("account-a1"))
    assert read_model["balance"] == 60


def test_write_path_and_read_path_are_separate_streams():
    store = EventStore()
    handler = CommandHandler(store)
    projection = BalanceProjection()

    handler.handle({"type": "Deposit", "account_id": "a1", "amount": 100})
    handler.handle({"type": "Deposit", "account_id": "a2", "amount": 7})

    assert projection.rebuild(store.read("account-a1"))["balance"] == 100
    assert projection.rebuild(store.read("account-a2"))["balance"] == 7


def test_withdraw_beyond_balance_is_rejected_and_appends_no_event():
    store = EventStore()
    handler = CommandHandler(store)

    handler.handle({"type": "Deposit", "account_id": "a1", "amount": 50})
    try:
        handler.handle({"type": "Withdraw", "account_id": "a1", "amount": 999})
        raised = False
    except CommandError:
        raised = True

    assert raised
    # The rejected command must not have leaked an event into the log.
    assert len(store.read("account-a1")) == 1


def test_invalid_command_is_rejected():
    handler = CommandHandler(EventStore())
    for bad in (
        {"type": "Frobnicate", "account_id": "a1", "amount": 1},
        {"type": "Deposit", "account_id": "", "amount": 1},
        {"type": "Deposit", "account_id": "a1", "amount": -1},
        {"type": "Deposit", "account_id": "a1", "amount": 0},
    ):
        try:
            handler.handle(bad)
            raised = False
        except CommandError:
            raised = True
        assert raised, f"expected rejection for {bad!r}"


if __name__ == "__main__":
    # Plain-assert runner so the suite works even without pytest installed.
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
