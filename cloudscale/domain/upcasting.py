"""Event schema evolution: translate stored events forward at read time.

ADR-0009. Stored events are never rewritten. Each event row carries the
``schema_version`` it was written with; readers call :func:`upcast` and
receive the payload in the *current* shape. Translation steps are pure
functions registered per ``(event_type, from_version)`` and are chained
until the payload reaches ``CURRENT_SCHEMA_VERSION[event_type]``.

Rules that keep the source of record safe:

* A step maps exactly one version to the next (``n -> n+1``). No skipping.
* Every version ever written must have a fixture in
  ``tests/fixtures/events/`` and must upcast and fold in the test suite.
* A version newer than this build understands is an error, never a guess.
* Until a type needs its first step, the registry for it is empty and
  ``upcast`` is the identity plus a version stamp.

This module is domain code: standard library only.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Final

__all__ = [
    "CURRENT_SCHEMA_VERSION",
    "UnknownSchemaVersionError",
    "Upcaster",
    "register_upcaster",
    "registered_steps",
    "upcast",
]

Upcaster = Callable[[dict[str, object]], dict[str, object]]

#: The shape this build writes and expects after upcasting, per event type.
#: Bump a type's version here in the SAME change that registers its step and
#: adds the new fixture.
CURRENT_SCHEMA_VERSION: Final[dict[str, int]] = {
    "Deposited": 1,
    "Withdrawn": 1,
}

_REGISTRY: dict[tuple[str, int], Upcaster] = {}


class UnknownSchemaVersionError(ValueError):
    """The stored version cannot be translated by this build."""


def register_upcaster(
    event_type: str, from_version: int
) -> Callable[[Upcaster], Upcaster]:
    """Register a step translating ``event_type`` from ``from_version`` to +1."""

    if event_type not in CURRENT_SCHEMA_VERSION:
        raise ValueError(f"unknown event type {event_type!r}")
    if not 1 <= from_version < CURRENT_SCHEMA_VERSION[event_type]:
        raise ValueError(
            f"{event_type} step from v{from_version} is outside "
            f"1..{CURRENT_SCHEMA_VERSION[event_type] - 1}"
        )
    key = (event_type, from_version)

    def decorate(fn: Upcaster) -> Upcaster:
        if key in _REGISTRY:
            raise ValueError(f"upcaster already registered for {key}")
        _REGISTRY[key] = fn
        return fn

    return decorate


def registered_steps() -> dict[tuple[str, int], Upcaster]:
    return dict(_REGISTRY)


def upcast(event: Mapping[str, object]) -> dict[str, object]:
    """Return ``event`` translated to the current schema for its type.

    ``schema_version`` absent means 1: rows written before the column existed
    are, by definition, the first shape. The result always carries the
    current version. Fields outside the payload (ids, stream, seq) pass
    through untouched.
    """

    event_type = event.get("type")
    if not isinstance(event_type, str) or event_type not in CURRENT_SCHEMA_VERSION:
        raise UnknownSchemaVersionError(f"unknown event type {event_type!r}")
    target = CURRENT_SCHEMA_VERSION[event_type]
    raw_version = event.get("schema_version", 1)
    if not isinstance(raw_version, int) or raw_version < 1:
        raise UnknownSchemaVersionError(
            f"{event_type}: invalid schema_version {raw_version!r}"
        )
    if raw_version > target:
        raise UnknownSchemaVersionError(
            f"{event_type} v{raw_version} is newer than this build's v{target}; "
            "upgrade the service before reading this event"
        )

    current: dict[str, object] = dict(event)
    version = raw_version
    while version < target:
        step = _REGISTRY.get((event_type, version))
        if step is None:
            raise UnknownSchemaVersionError(
                f"no upcaster registered for {event_type} v{version} -> v{version + 1}"
            )
        current = dict(step(current))
        version += 1
    current["schema_version"] = target
    return current
