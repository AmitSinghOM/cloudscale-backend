"""Query side: fold a stream of events into a read model.

Phase 0 stand-in for the PostgreSQL read models in the README. A projection is
a pure fold over events, so a read model can always be rebuilt by replaying
the log from the start.
"""

from __future__ import annotations

from typing import Dict, List


class BalanceProjection:
    """Folds account events into a ``{account_id, balance, version}`` dict."""

    def initial(self) -> Dict:
        return {"account_id": None, "balance": 0, "version": 0}

    def apply(self, state: Dict, event: dict) -> Dict:
        """Return the next read-model state after applying ``event``."""
        etype = event.get("type")
        if state["account_id"] is None:
            state["account_id"] = event.get("account_id")

        if etype == "Deposited":
            state["balance"] += event["amount"]
        elif etype == "Withdrawn":
            state["balance"] -= event["amount"]
        # Unknown event types are ignored so projections stay forward-compatible.

        state["version"] += 1
        return state

    def rebuild(self, events: List[dict]) -> Dict:
        """Rebuild the read model from scratch by replaying ``events``."""
        state = self.initial()
        for event in events:
            state = self.apply(state, event)
        return state
