"""The ``transfers`` read model (ADR-0015): what each movement event does to it.

Both projection stores (PostgreSQL, SQLite) call :func:`transfer_row_effect`
so the two tiers cannot drift. A posting set is one ``transfers`` row keyed by
its ``transfer_id`` plus one ``transfer_legs`` row per account (the legs of a
set are appended in one transaction but projected one event at a time, so
each leg is inserted idempotently as it arrives); a reversal additionally
marks the set it mirrors as ``reverted_by`` itself.
"""

from __future__ import annotations

from dataclasses import dataclass

from cloudscale.domain.events import BALANCE_SIGN

_KINDS = {
    "TransferDebited": "transfer",
    "TransferCredited": "transfer",
    "HoldPosted": "hold_posting",
    "ReversalDebited": "reversal",
    "ReversalCredited": "reversal",
}


@dataclass(frozen=True, slots=True)
class TransferRowEffect:
    """Upsert the set row, insert this leg, and mark ``marks_reverted`` if a reversal."""

    transfer_id: str
    kind: str
    account_id: str
    amount: int
    direction: str
    reverts: str | None

    @property
    def marks_reverted(self) -> str | None:
        return self.reverts


def transfer_row_effect(event: dict) -> TransferRowEffect | None:
    """Return the read-model effect of one event row, or ``None`` if it has none."""
    event_type = str(event.get("type"))
    kind = _KINDS.get(event_type)
    transfer_id = event.get("transfer_id")
    if kind is None or transfer_id is None:
        return None
    reverts = event.get("reverts")
    return TransferRowEffect(
        transfer_id=str(transfer_id),
        kind=kind,
        account_id=str(event["account_id"]),
        amount=int(event["amount"]),
        direction="debit" if BALANCE_SIGN[event_type] < 0 else "credit",
        reverts=str(reverts) if reverts else None,
    )


def kind_for_leg_count(kind: str, legs: int) -> str:
    """A ``transfer`` with more than two legs is a ``posting`` set."""
    if kind == "transfer" and legs > 2:
        return "posting"
    return kind


__all__ = ["TransferRowEffect", "kind_for_leg_count", "transfer_row_effect"]
