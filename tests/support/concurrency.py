"""Deterministic concurrency coordination for integration and model tests."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


class BarrierError(RuntimeError):
    """Base error for deterministic barrier misuse or failure."""


class BarrierTimeout(BarrierError, TimeoutError):
    """Raised for every waiter when a barrier generation times out."""


class ConcurrencyTimeout(TimeoutError):
    """Raised when coordinated workers do not complete by their deadline."""

    def __init__(self, pending: Sequence[str]) -> None:
        super().__init__(f"coordinated workers timed out: {', '.join(pending)}")
        self.pending = tuple(pending)


@dataclass(frozen=True)
class BarrierPass:
    """One participant's deterministic release metadata."""

    name: str
    generation: int
    ordinal: int
    arrived_monotonic_ns: int
    released_monotonic_ns: int


@dataclass(frozen=True)
class BarrierCycle:
    """Recorded arrivals for a completed barrier generation."""

    generation: int
    released_monotonic_ns: int
    arrivals: tuple[tuple[str, int], ...]


class DeterministicBarrier:
    """Reusable named-participant barrier with explicit timeout failure.

    Unlike ``threading.Barrier``, the returned ordinal is tied to declared
    participant order rather than scheduler arrival order, making evidence and
    assertions reproducible across runs.
    """

    def __init__(
        self,
        participants: Sequence[str],
        *,
        name: str = "barrier",
        timeout: float = 5.0,
    ) -> None:
        declared = tuple(participants)
        if not declared or any(not item for item in declared):
            raise ValueError("participants must contain non-empty names")
        if len(set(declared)) != len(declared):
            raise ValueError("participant names must be unique")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.name = name
        self.participants = declared
        self.timeout = timeout
        self.history: list[BarrierCycle] = []
        self._ordinals = {
            participant: index for index, participant in enumerate(declared)
        }
        self._condition = threading.Condition()
        self._generation = 0
        self._arrivals: dict[str, int] = {}
        self._completed: dict[int, BarrierCycle] = {}
        self._broken: BarrierError | None = None

    @property
    def generation(self) -> int:
        with self._condition:
            return self._generation

    @property
    def broken(self) -> bool:
        with self._condition:
            return self._broken is not None

    def wait(self, participant: str, timeout: float | None = None) -> BarrierPass:
        """Arrive once in the current generation and wait for every peer."""
        if participant not in self._ordinals:
            raise BarrierError(f"unknown participant {participant!r} for {self.name}")
        deadline = time.monotonic() + (self.timeout if timeout is None else timeout)
        with self._condition:
            if self._broken is not None:
                raise self._broken
            generation = self._generation
            if participant in self._arrivals:
                raise BarrierError(
                    f"participant {participant!r} arrived twice in generation {generation}"
                )
            arrived_ns = time.monotonic_ns()
            self._arrivals[participant] = arrived_ns

            if len(self._arrivals) == len(self.participants):
                released_ns = time.monotonic_ns()
                cycle = BarrierCycle(
                    generation=generation,
                    released_monotonic_ns=released_ns,
                    arrivals=tuple(
                        (name, self._arrivals[name]) for name in self.participants
                    ),
                )
                self.history.append(cycle)
                self._completed[generation] = cycle
                self._arrivals = {}
                self._generation += 1
                self._condition.notify_all()
            else:
                while generation == self._generation and self._broken is None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self._break(
                            BarrierTimeout(
                                f"{self.name} generation {generation} timed out; "
                                f"arrived={sorted(self._arrivals)}"
                            )
                        )
                        break
                    self._condition.wait(remaining)

            if self._broken is not None:
                raise self._broken
            cycle = self._completed[generation]
            return BarrierPass(
                name=participant,
                generation=generation,
                ordinal=self._ordinals[participant],
                arrived_monotonic_ns=arrived_ns,
                released_monotonic_ns=cycle.released_monotonic_ns,
            )

    def abort(self, reason: str = "barrier aborted") -> None:
        """Wake all current waiters with a deterministic error."""
        with self._condition:
            self._break(BarrierError(f"{self.name}: {reason}"))

    def _break(self, error: BarrierError) -> None:
        self._broken = error
        self._condition.notify_all()


@dataclass(frozen=True)
class WorkerResult(Generic[T]):
    """Value and timing for one coordinated worker."""

    name: str
    value: T
    started_monotonic_ns: int
    ended_monotonic_ns: int
    thread_id: int

    @property
    def duration_ms(self) -> float:
        return (self.ended_monotonic_ns - self.started_monotonic_ns) / 1_000_000


def run_concurrently(
    workers: Mapping[str, Callable[[], T]],
    *,
    timeout: float = 10.0,
    barrier: DeterministicBarrier | None = None,
) -> list[WorkerResult[T]]:
    """Release named workers together and return results in declared order."""
    if not workers:
        return []
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    names = tuple(workers)
    start_barrier = barrier or DeterministicBarrier(
        names, name="worker-start", timeout=timeout
    )
    if start_barrier.participants != names:
        raise ValueError("barrier participants must match worker declaration order")

    def invoke(name: str) -> WorkerResult[T]:
        start_barrier.wait(name)
        started_ns = time.monotonic_ns()
        value = workers[name]()
        ended_ns = time.monotonic_ns()
        return WorkerResult(
            name=name,
            value=value,
            started_monotonic_ns=started_ns,
            ended_monotonic_ns=ended_ns,
            thread_id=threading.get_ident(),
        )

    executor = ThreadPoolExecutor(
        max_workers=len(names), thread_name_prefix="coordinated"
    )
    futures: dict[str, Future[WorkerResult[T]]] = {
        name: executor.submit(invoke, name) for name in names
    }
    timed_out = False
    try:
        _, incomplete = wait(futures.values(), timeout=timeout)
        if incomplete:
            timed_out = True
            start_barrier.abort("worker deadline expired")
            pending = [name for name in names if futures[name] in incomplete]
            for future in incomplete:
                future.cancel()
            raise ConcurrencyTimeout(pending)
        return [futures[name].result() for name in names]
    finally:
        executor.shutdown(wait=not timed_out, cancel_futures=True)
