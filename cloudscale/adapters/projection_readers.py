"""Adapt the balance stores to the application-layer ``ProjectionReader`` port."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from cloudscale.domain.results import BalanceView, TransferLeg, TransferView


class _BalanceStore(Protocol):
    """Structural subset both projection stores expose."""

    def balance(self, account_id: str) -> dict: ...

    def transfer(self, transfer_id: str) -> dict | None: ...


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
            held=int(row.get("held", 0)),
        )

    def get_transfer(self, transfer_id: UUID) -> TransferView | None:
        row = self._store.transfer(str(transfer_id))
        if row is None:
            return None
        legs = row["legs"]
        return TransferView(
            transfer_id=UUID(str(row["transfer_id"])),
            kind=str(row["kind"]),
            legs=tuple(
                TransferLeg(
                    str(leg["account_id"]), int(leg["amount"]), str(leg["direction"])
                )
                for leg in legs
            ),
            reverted_by=UUID(row["reverted_by"]) if row.get("reverted_by") else None,
            reverts=UUID(row["reverts"]) if row.get("reverts") else None,
        )


__all__ = ["StoreProjectionReader"]
