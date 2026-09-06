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
    """Validates account commands and appends events to the event store."""

    def __init__(self, store: EventStore) -> None:
        self._store = store
        self._handlers: Dict[str, Callable[[str, dict], dict]] = {
            "Deposit": self._handle_deposit,
            "Withdraw": self._handle_withdraw,
        }

    def _stream(self, account_id: str) -> str:
        return f"account-{account_id}"

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

        event = self._handlers[ctype](account_id, command)
        return self._store.append(self._stream(account_id), event)

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
        # Enforce the no-overdraft rule by replaying current state.
        current = BalanceProjection().rebuild(
            self._store.read(self._stream(account_id))
        )
        if amount > current["balance"]:
            raise CommandError("insufficient funds")
        return {"type": "Withdrawn", "account_id": account_id, "amount": amount}
