"""In-memory, append-only event log.

Phase 0 stand-in for the Kafka event log described in the README. The write
path appends immutable events to per-stream logs; the read path replays them.
Deliberately stdlib-only and in-process.
"""

from __future__ import annotations

from threading import Lock
from typing import Dict, List


class EventStore:
    """Append-only event log keyed by stream id.

    Events are stored as plain dicts. Each append returns a monotonically
    increasing per-stream sequence number (1-based). The store never mutates
    or removes events once written.
    """

    def __init__(self) -> None:
        self._streams: Dict[str, List[dict]] = {}
        self._lock = Lock()

    def append(self, stream: str, event: dict) -> int:
        """Append ``event`` to ``stream`` and return its sequence number.

        The event dict is copied and stamped with its ``seq`` so callers
        cannot mutate stored state after the fact.
        """
        if not isinstance(stream, str) or not stream:
            raise ValueError("stream must be a non-empty string")
        if not isinstance(event, dict):
            raise TypeError("event must be a dict")

        with self._lock:
            log = self._streams.setdefault(stream, [])
            seq = len(log) + 1
            stored = dict(event)
            stored["seq"] = seq
            log.append(stored)
            return seq

    def read(self, stream: str) -> List[dict]:
        """Return a copy of all events in ``stream`` in append order."""
        with self._lock:
            return [dict(e) for e in self._streams.get(stream, [])]
