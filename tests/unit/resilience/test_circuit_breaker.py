"""Deterministic tests for the CircuitBreaker state machine."""

from __future__ import annotations

import pytest

from cloudscale.resilience import CircuitBreaker, CircuitOpenError, CircuitState


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _failing(error: BaseException):
    def operation() -> None:
        raise error

    return operation


def _breaker(
    clock: _Clock, threshold: int = 3, timeout: float = 30.0
) -> CircuitBreaker:
    return CircuitBreaker(
        failure_threshold=threshold,
        reset_timeout_seconds=timeout,
        clock=clock,
        counted_errors=(TimeoutError,),
    )


def test_invalid_configuration_is_rejected() -> None:
    with pytest.raises(ValueError):
        CircuitBreaker(failure_threshold=0)
    with pytest.raises(ValueError):
        CircuitBreaker(reset_timeout_seconds=0.0)
    with pytest.raises(ValueError):
        CircuitBreaker(counted_errors=())


def test_stays_closed_below_threshold_and_success_resets_the_count() -> None:
    clock = _Clock()
    breaker = _breaker(clock, threshold=3)
    for _ in range(2):
        with pytest.raises(TimeoutError):
            breaker.call(_failing(TimeoutError("down")))
    assert breaker.state is CircuitState.CLOSED
    assert breaker.call(lambda: "ok") == "ok"  # resets consecutive count
    for _ in range(2):
        with pytest.raises(TimeoutError):
            breaker.call(_failing(TimeoutError("down")))
    assert breaker.state is CircuitState.CLOSED


def test_opens_at_threshold_and_fails_fast_without_calling() -> None:
    clock = _Clock()
    breaker = _breaker(clock, threshold=3, timeout=30.0)
    for _ in range(3):
        with pytest.raises(TimeoutError):
            breaker.call(_failing(TimeoutError("down")))
    assert breaker.state is CircuitState.OPEN

    def must_not_run() -> None:
        pytest.fail("operation must not be called while open")

    clock.advance(10.0)
    with pytest.raises(CircuitOpenError) as excinfo:
        breaker.call(must_not_run)
    assert excinfo.value.retry_after_seconds == pytest.approx(20.0)


def test_half_open_probe_success_closes_the_circuit() -> None:
    clock = _Clock()
    breaker = _breaker(clock, threshold=1, timeout=30.0)
    with pytest.raises(TimeoutError):
        breaker.call(_failing(TimeoutError("down")))
    clock.advance(30.0)
    assert breaker.state is CircuitState.HALF_OPEN
    assert breaker.call(lambda: "recovered") == "recovered"
    assert breaker.state is CircuitState.CLOSED


def test_half_open_probe_failure_reopens_and_restarts_the_timeout() -> None:
    clock = _Clock()
    breaker = _breaker(clock, threshold=1, timeout=30.0)
    with pytest.raises(TimeoutError):
        breaker.call(_failing(TimeoutError("down")))
    clock.advance(30.0)
    with pytest.raises(TimeoutError):
        breaker.call(_failing(TimeoutError("still down")))
    assert breaker.state is CircuitState.OPEN
    clock.advance(29.9)
    with pytest.raises(CircuitOpenError):
        breaker.call(lambda: "nope")
    clock.advance(0.1)
    assert breaker.state is CircuitState.HALF_OPEN


def test_uncounted_error_propagates_but_records_success() -> None:
    clock = _Clock()
    breaker = _breaker(clock, threshold=1)
    # A deterministic rejection proves the downstream processed the call:
    # it must not trip a threshold-1 breaker.
    with pytest.raises(ValueError):
        breaker.call(_failing(ValueError("domain rejection")))
    assert breaker.state is CircuitState.CLOSED
    # And it resets the consecutive-failure count accumulated so far.
    breaker2 = _breaker(clock, threshold=2)
    with pytest.raises(TimeoutError):
        breaker2.call(_failing(TimeoutError("down")))
    with pytest.raises(ValueError):
        breaker2.call(_failing(ValueError("processed fine")))
    with pytest.raises(TimeoutError):
        breaker2.call(_failing(TimeoutError("down")))
    assert breaker2.state is CircuitState.CLOSED
