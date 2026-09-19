"""Stream snapshots: a verified cache over the event log (ADR-0012).

A snapshot row says *"the fold of this stream's events with ``seq <= seq``
is ``state``, computed by a build whose ``AccountState`` shape is
``state_version``, and the event at ``(stream, seq)`` had ``anchor_event_id``"*.
It is derived data. Readers use it only when every one of those claims can
be checked against the log; otherwise they fold the full stream and say why.

This module is storage-agnostic: adapters read and write rows, this module
decides whether a row may be trusted and when a new one is due. Both units
of work share it so the two tiers cannot drift.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from cloudscale.domain.account import CURRENT_STATE_VERSION, AccountState, apply, fold
from cloudscale.domain.events import AccountEvent, EventEnvelope

LOGGER = logging.getLogger("cloudscale.snapshots")

DEFAULT_SNAPSHOT_EVERY = 100


@dataclass(frozen=True, slots=True)
class StreamSnapshot:
    """One stored snapshot row, storage-neutral."""

    seq: int
    state: AccountState
    state_version: int
    anchor_event_id: str

    def to_state_json(self) -> str:
        return json.dumps(
            {
                "account_id": self.state.account_id,
                "balance": self.state.balance,
                "version": self.state.version,
                "held": self.state.held,
            },
            separators=(",", ":"),
        )

    @staticmethod
    def state_from_json(text: str) -> AccountState:
        data = json.loads(text)
        return AccountState(
            account_id=data["account_id"],
            balance=int(data["balance"]),
            version=int(data["version"]),
            held=int(data.get("held", 0)),
        )


def snapshot_rejection(
    snapshot: StreamSnapshot, rows: Sequence[Mapping[str, Any]]
) -> str | None:
    """Return why ``snapshot`` must not be used, or ``None`` if it may.

    ``rows`` are the stream's events with ``seq >= snapshot.seq`` in order;
    the first must be the anchor. Every reason names a real-world cause so an
    operator reading the WARNING knows what happened to the log.
    """
    if snapshot.state_version != CURRENT_STATE_VERSION:
        return (
            f"state_version {snapshot.state_version} != build "
            f"{CURRENT_STATE_VERSION} (AccountState shape or fold semantics changed)"
        )
    if snapshot.state.version != snapshot.seq:
        return f"state.version {snapshot.state.version} != seq {snapshot.seq}"
    if not rows:
        return f"anchor seq {snapshot.seq} is past the end of the stream (truncated?)"
    first = rows[0]
    if (
        int(first["seq"]) != snapshot.seq
        or str(first["event_id"]) != snapshot.anchor_event_id
    ):
        return (
            f"anchor mismatch at seq {snapshot.seq}: log has "
            f"seq={first['seq']} event_id={first['event_id']}, snapshot expected "
            f"event_id={snapshot.anchor_event_id} (restored from a different log?)"
        )
    return None


def fold_from_snapshot(
    snapshot: StreamSnapshot, tail: Sequence[AccountEvent]
) -> AccountState:
    """Fold ``tail`` (events strictly after the anchor) onto the snapshot state."""
    return fold(tail, initial_state=snapshot.state)


class SnapshotTracker:
    """Per-transaction bookkeeping: which streams were folded, and from where.

    The unit of work calls :meth:`folded` after each ``fold_stream`` and
    :meth:`after_append` after each ``append_event``; the latter returns the
    snapshot row to upsert when the stream has advanced ``every`` events past
    its last snapshot, or ``None``. ``every <= 0`` disables writing.
    """

    def __init__(self, every: int) -> None:
        self._every = every
        self._states: dict[str, AccountState] = {}
        self._snapshot_seq: dict[str, int] = {}

    def folded(self, account_id: str, state: AccountState, snapshot_seq: int) -> None:
        self._states[account_id] = state
        self._snapshot_seq[account_id] = snapshot_seq

    def after_append(
        self, envelope: EventEnvelope, event: AccountEvent
    ) -> StreamSnapshot | None:
        prior = self._states.get(event.account_id)
        if prior is None:  # pragma: no cover - every append follows a fold
            return None
        new_state = apply(prior, event)
        self._states[event.account_id] = new_state
        if self._every <= 0:
            return None
        if envelope.stream_version - self._snapshot_seq[event.account_id] < self._every:
            return None
        self._snapshot_seq[event.account_id] = envelope.stream_version
        return StreamSnapshot(
            seq=envelope.stream_version,
            state=new_state,
            state_version=CURRENT_STATE_VERSION,
            anchor_event_id=str(envelope.event_id),
        )


def log_snapshot_rejected(stream: str, reason: str) -> None:
    LOGGER.warning(
        "snapshot.rejected stream=%s reason=%s (folding full stream)", stream, reason
    )


__all__ = [
    "DEFAULT_SNAPSHOT_EVERY",
    "SnapshotTracker",
    "StreamSnapshot",
    "fold_from_snapshot",
    "log_snapshot_rejected",
    "snapshot_rejection",
]
