"""Contracts for the memoized withdraw guard and the read_after store API.

The fix (docs/phase-3-bottleneck-withdraw-guard.md) must be a pure
memoization: for any command sequence, decisions and appended events are
identical to the original full-replay implementation.
"""

from __future__ import annotations

import random

import pytest

from cqrs import CommandError, CommandHandler, EventStore, SqliteEventStore
from cqrs.projections import BalanceProjection

# -- read_after ---------------------------------------------------------------


@pytest.mark.parametrize(
    "make_store", [EventStore, lambda: SqliteEventStore(":memory:")]
)
def test_read_after_returns_the_exact_suffix(make_store) -> None:
    store = make_store()
    for index in range(1, 6):
        store.append("s-1", {"type": "Deposited", "account_id": "a", "amount": index})
    store.append("s-2", {"type": "Deposited", "account_id": "b", "amount": 99})

    assert store.read_after("s-1", 0) == store.read("s-1")
    suffix = store.read_after("s-1", 3)
    assert [event["seq"] for event in suffix] == [4, 5]
    assert [event["amount"] for event in suffix] == [4, 5]
    assert store.read_after("s-1", 5) == []
    assert store.read_after("missing", 0) == []
    with pytest.raises(ValueError):
        store.read_after("s-1", -1)


# -- reference implementation (the original full-replay guard) -----------------


def _replay_reference_handle(store, command: dict) -> int:
    """The pre-fix decision procedure, verbatim semantics."""
    ctype = command.get("type")
    account_id = command["account_id"]
    stream = f"account-{account_id}"
    amount = command["amount"]
    if ctype == "Withdraw":
        current = BalanceProjection().rebuild(store.read(stream))
        if amount > current["balance"]:
            raise CommandError("insufficient funds")
        event = {"type": "Withdrawn", "account_id": account_id, "amount": amount}
    else:
        event = {"type": "Deposited", "account_id": account_id, "amount": amount}
    return store.append(stream, event)


def _random_commands(seed: int, count: int) -> list[dict]:
    rng = random.Random(seed)
    commands = []
    for _ in range(count):
        commands.append(
            {
                "type": rng.choice(["Deposit", "Withdraw", "Withdraw"]),
                "account_id": rng.choice(["a", "b"]),
                "amount": rng.randint(1, 40),
            }
        )
    return commands


@pytest.mark.parametrize("seed", [7, 41609, 20260908])
def test_memoized_guard_is_decision_identical_to_full_replay(seed: int) -> None:
    commands = _random_commands(seed, 300)

    memo_store = EventStore()
    handler = CommandHandler(memo_store)
    reference_store = EventStore()

    for command in commands:
        try:
            handler.handle(dict(command))
            memo_outcome = "accepted"
        except CommandError as error:
            memo_outcome = f"rejected: {error}"
        try:
            _replay_reference_handle(reference_store, dict(command))
            reference_outcome = "accepted"
        except CommandError as error:
            reference_outcome = f"rejected: {error}"
        assert memo_outcome == reference_outcome, command

    for account in ("a", "b"):
        stream = f"account-{account}"
        assert memo_store.read(stream) == reference_store.read(stream)


# -- external writers and boundary behavior ------------------------------------


def test_guard_sees_events_appended_by_another_handler() -> None:
    store = SqliteEventStore(":memory:")
    ours = CommandHandler(store)
    theirs = CommandHandler(store)

    ours.handle({"type": "Deposit", "account_id": "a", "amount": 10})
    with pytest.raises(CommandError):
        ours.handle({"type": "Withdraw", "account_id": "a", "amount": 15})

    # A different handler instance funds the account behind our memo's back.
    theirs.handle({"type": "Deposit", "account_id": "a", "amount": 10})

    # Our catch-up read must fold the unseen deposit before deciding.
    assert ours.handle({"type": "Withdraw", "account_id": "a", "amount": 15}) == 3


def test_guard_sees_events_appended_directly_to_the_store() -> None:
    store = EventStore()
    handler = CommandHandler(store)
    handler.handle({"type": "Deposit", "account_id": "a", "amount": 5})
    # Bypass the handler entirely (e.g. a migration or another writer).
    store.append("account-a", {"type": "Deposited", "account_id": "a", "amount": 20})
    assert handler.handle({"type": "Withdraw", "account_id": "a", "amount": 25}) == 3
    with pytest.raises(CommandError):
        handler.handle({"type": "Withdraw", "account_id": "a", "amount": 1})


def test_overdraft_boundary_is_exact() -> None:
    handler = CommandHandler(EventStore())
    handler.handle({"type": "Deposit", "account_id": "a", "amount": 100})
    assert handler.handle({"type": "Withdraw", "account_id": "a", "amount": 100}) == 2
    with pytest.raises(CommandError, match="insufficient funds"):
        handler.handle({"type": "Withdraw", "account_id": "a", "amount": 1})
