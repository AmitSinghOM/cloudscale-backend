"""OpenTelemetry tracing over the hot path, exporting in-process.

Adapters may depend on frameworks; the domain, application, and ``cqrs``
layers stay untouched — tracing wraps the seams from the outside via
subclassing. The default exporter is in-memory so the load harness can trace
without a collector on the host; swapping in an OTLP exporter later is a
one-line change at :func:`configure_in_memory_tracing` call sites.

Span model of the hot path:

    command.handle                (created by the caller, e.g. the harness)
      eventstore.read_after       (withdraw-guard catch-up read)
      eventstore.append           (durable write)
    consumer.catchup              (created by the caller)
      projection.apply            (idempotent read-model mutation)
"""

from __future__ import annotations

import statistics
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from cloudscale.adapters.sqlite_compat.dead_letter_store import (
    DeadLetteringProjectionStore,
)
from cqrs import SqliteEventStore


def configure_in_memory_tracing(
    service_name: str,
) -> tuple[trace.Tracer, InMemorySpanExporter]:
    """Return a tracer and the in-memory exporter capturing its spans.

    The provider is local to the caller (not installed globally), so tests
    and harness runs cannot leak spans into each other.
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer(service_name), exporter


class TracedSqliteEventStore(SqliteEventStore):
    """Durable event store emitting a span per append and guard read."""

    def __init__(self, path: str, tracer: trace.Tracer) -> None:
        super().__init__(path)
        self._tracer = tracer

    def append(self, stream: str, event: dict) -> int:
        with self._tracer.start_as_current_span(
            "eventstore.append", attributes={"stream": stream}
        ):
            return super().append(stream, event)

    def read_after(self, stream: str, after_seq: int) -> list[dict]:
        with self._tracer.start_as_current_span(
            "eventstore.read_after",
            attributes={"stream": stream, "after_seq": after_seq},
        ):
            return super().read_after(stream, after_seq)


class TracedDeadLetteringProjectionStore(DeadLetteringProjectionStore):
    """Projection store emitting a span per idempotent apply."""

    def __init__(
        self, path: str, tracer: trace.Tracer, consumer: str = "balances"
    ) -> None:
        super().__init__(path=path, consumer=consumer)
        self._tracer = tracer

    def apply(self, event: dict) -> bool:
        with self._tracer.start_as_current_span("projection.apply") as span:
            mutated = super().apply(event)
            span.set_attribute("duplicate", not mutated)
            return mutated


def summarize_spans(exporter: InMemorySpanExporter) -> dict:
    """Aggregate finished spans per name: count and duration statistics (ms)."""
    durations_ms: dict[str, list[float]] = {}
    for span in exporter.get_finished_spans():
        if span.end_time is None or span.start_time is None:
            continue
        durations_ms.setdefault(span.name, []).append(
            (span.end_time - span.start_time) / 1_000_000
        )

    summary = {}
    for name, values in sorted(durations_ms.items()):
        values.sort()
        p95_index = min(len(values) - 1, int(0.95 * len(values)))
        summary[name] = {
            "count": len(values),
            "total_ms": round(sum(values), 3),
            "mean_ms": round(statistics.fmean(values), 4),
            "p95_ms": round(values[p95_index], 4),
            "max_ms": round(values[-1], 4),
        }
    return summary


__all__ = [
    "TracedDeadLetteringProjectionStore",
    "TracedSqliteEventStore",
    "configure_in_memory_tracing",
    "summarize_spans",
]
