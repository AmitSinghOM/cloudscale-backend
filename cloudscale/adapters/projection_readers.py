"""Adapt the balance stores to the application-layer ``ProjectionReader`` port."""

from __future__ import annotations

from typing import Protocol

from cloudscale.domain.results import BalanceView


class _BalanceStore(Protocol):
    """Structural subset both projection stores expose."""

    def balance(self, account_id: str) -> dict: ...


class StoreProjectionReader:
    """``ProjectionReader`` over the SQLite or Postgres projection store.

    Both stores return a zero row for unknown accounts; version 0 is
    impossible for a real account (the first applied event sets version 1),
    so a version-0 row maps to ``None`` — the port's absence signal.
    """

    def __init__(self, store: _BalanceStore) -> None:
        self._store = store

    def get_balance(self, account_id: str) -> BalanceView | None:
        row = self._store.balance(account_id)
        if int(row["version"]) == 0:
            return None
        return BalanceView(
            account_id=str(row["account_id"]),
            balance=int(row["balance"]),
            version=int(row["version"]),
        )


__all__ = ["StoreProjectionReader"]
