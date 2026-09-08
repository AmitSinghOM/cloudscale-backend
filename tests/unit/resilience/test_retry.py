"""Deterministic tests for RetryPolicy and call_with_retry."""

from __future__ import annotations

import pytest

from cloudscale.resilience import RetryPolicy, call_with_retry


class _Flaky:
    """Fails ``failures`` times with ``error``, then returns ``result``."""

    def __init__(self, failures: int, error: BaseException, result: str = "ok"):
        self.failures = failures
        self.error = error
        self.result = result
        self.calls = 0

    def __call__(self) -> str:
        self.calls += 1
        if self.calls <= self.failures:
            raise self.error
        return self.result


def test_policy_rejects_invalid_budgets() -> None:
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError):
        RetryPolicy(base_delay_seconds=-0.1)
    with pytest.raises(ValueError):
        RetryPolicy(base_delay_seconds=2.0, max_delay_seconds=1.0)


def test_backoff_ceiling_doubles_then_caps() -> None:
    policy = RetryPolicy(max_attempts=6, base_delay_seconds=1.0, max_delay_seconds=5.0)
    assert [policy.backoff_ceiling(n) for n in range(1, 6)] == [
        1.0,
        2.0,
        4.0,
        5.0,
        5.0,
    ]
    with pytest.raises(ValueError):
        policy.backoff_ceiling(0)


def test_success_after_transient_failures_with_exact_jittered_sleeps() -> None:
    sleeps: list[float] = []
    operation = _Flaky(failures=2, error=TimeoutError("busy"))
    result = call_with_retry(
        operation,
        RetryPolicy(max_attempts=3, base_delay_seconds=1.0, max_delay_seconds=8.0),
        retryable_errors=(TimeoutError,),
        sleep=sleeps.append,
        random_fn=lambda: 0.5,
    )
    assert result == "ok"
    assert operation.calls == 3
    assert sleeps == [0.5, 1.0]  # 0.5 * 1.0, 0.5 * 2.0


def test_exhaustion_reraises_the_last_retryable_error() -> None:
    sleeps: list[float] = []
    operation = _Flaky(failures=5, error=TimeoutError("still busy"))
    with pytest.raises(TimeoutError, match="still busy"):
        call_with_retry(
            operation,
            RetryPolicy(max_attempts=3, base_delay_seconds=1.0),
            retryable_errors=(TimeoutError,),
            sleep=sleeps.append,
            random_fn=lambda: 1.0,
        )
    assert operation.calls == 3
    assert len(sleeps) == 2  # no sleep after the final attempt


def test_non_retryable_error_propagates_on_first_attempt() -> None:
    operation = _Flaky(failures=5, error=ValueError("poison"))
    with pytest.raises(ValueError, match="poison"):
        call_with_retry(
            operation,
            RetryPolicy(max_attempts=3),
            retryable_errors=(TimeoutError,),
            sleep=lambda _: pytest.fail("must not sleep"),
        )
    assert operation.calls == 1


def test_single_attempt_policy_never_sleeps() -> None:
    operation = _Flaky(failures=1, error=TimeoutError("busy"))
    with pytest.raises(TimeoutError):
        call_with_retry(
            operation,
            RetryPolicy(max_attempts=1),
            retryable_errors=(TimeoutError,),
            sleep=lambda _: pytest.fail("must not sleep"),
        )
    assert operation.calls == 1


def test_empty_retryable_errors_is_rejected() -> None:
    with pytest.raises(ValueError):
        call_with_retry(lambda: "ok", RetryPolicy(), retryable_errors=())
