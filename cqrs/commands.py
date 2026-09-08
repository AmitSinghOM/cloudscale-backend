"""Command side: validate a command, append the resulting event.

Worked example: an Account aggregate with Deposit and Withdraw commands. The
handler validates intent and business rules, then appends a single event to
the account's stream. It never touches read models directly (that is the
projection's job).
"""

from __future__ import annotations

from typing import Callable, Dict

from .eventstore import EventStore
from .projections import BalanceProjection


class CommandError(ValueError):
    """Raised when a command fails validation or a business rule."""


class CommandHandler:
    """Validates account commands and appends events to the event store.

    The no-overdraft decision uses a memoized fold: per stream the handler
    keeps ``(last_seq, folded_state)`` and, before every decision, folds only
    the events it has not seen yet (``store.read_after``). The state used is
    byte-for-byte the same fold a full replay produces — memoization, not
    approximation — so decisions are identical to the replay implementation
    while the per-command cost drops from O(n) to O(delta). The memo is
    process-local and rebuilt from the log on first touch, so there is no new
    durable state and no cache to corrupt.
    """

    def __init__(self, store: EventStore) -> None:
        self._store = store
        self._projection = BalanceProjection()
        # stream -> (seq of last folded event, folded projection state)
        self._folds: Dict[str, tuple[int, dict]] = {}
        self._handlers: Dict[str, Callable[[str, dict], dict]] = {
            "Deposit": self._handle_deposit,
            "Withdraw": self._handle_withdraw,
        }

    def _stream(self, account_id: str) -> str:
        return f"account-{account_id}"

    def _current_state(self, stream: str) -> tuple[int, dict]:
        """Return ``(last_seq, state)`` after folding all unseen events."""
        last_seq, state = self._folds.get(stream, (0, self._projection.initial()))
        for event in self._store.read_after(stream, last_seq):
            state = self._projection.apply(state, event)
            last_seq = int(event["seq"])
        self._folds[stream] = (last_seq, state)
        return last_seq, state

    def _record_append(self, stream: str, event: dict, seq: int) -> None:
        """Advance the memo with the event this handler just appended."""
        last_seq, state = self._folds.get(stream, (0, self._projection.initial()))
        if seq == last_seq + 1:
            stamped = dict(event)
            stamped["seq"] = seq
            self._folds[stream] = (seq, self._projection.apply(state, stamped))
        # A gap means another writer appended concurrently; leave the memo
        # behind — the next catch-up read folds the missing suffix.

    def handle(self, command: dict) -> int:
        """Validate ``command`` and append its event; return the event seq."""
        if not isinstance(command, dict):
            raise CommandError("command must be a dict")

        ctype = command.get("type")
        if ctype not in self._handlers:
            raise CommandError(f"unknown command type: {ctype!r}")

        account_id = command.get("account_id")
        if not isinstance(account_id, str) or not account_id:
            raise CommandError("command requires a non-empty account_id")

        stream = self._stream(account_id)
        event = self._handlers[ctype](account_id, command)
        seq = self._store.append(stream, event)
        self._record_append(stream, event, seq)
        return seq

    def _amount(self, command: dict) -> int:
        amount = command.get("amount")
        if not isinstance(amount, int) or isinstance(amount, bool):
            raise CommandError("amount must be an integer (minor units)")
        if amount <= 0:
            raise CommandError("amount must be positive")
        return amount

    def _handle_deposit(self, account_id: str, command: dict) -> dict:
        amount = self._amount(command)
        return {"type": "Deposited", "account_id": account_id, "amount": amount}

    def _handle_withdraw(self, account_id: str, command: dict) -> dict:
        amount = self._amount(command)
        # Enforce the no-overdraft rule against the memoized fold, caught up
        # to the head of the stream (identical state to a full replay).
        _, current = self._current_state(self._stream(account_id))
        if amount > current["balance"]:
            raise CommandError("insufficient funds")
        return {"type": "Withdrawn", "account_id": account_id, "amount": amount}
