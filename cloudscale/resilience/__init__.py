"""Pure resilience primitives: retry with backoff and a circuit breaker.

This subpackage depends only on the standard library — never on
``cloudscale.adapters``, ``cloudscale.processes``, ``cqrs``, or frameworks —
so it stays reusable at any layer.
"""

from cloudscale.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
)
from cloudscale.resilience.retry import RetryPolicy, call_with_retry

__all__ = [
    "CircuitBreaker",
    "CircuitOpenError",
    "CircuitState",
    "RetryPolicy",
    "call_with_retry",
]
