"""Tracing adapter contracts: span emission, parentage, summary, isolation."""

from __future__ import annotations

from cloudscale.adapters.telemetry import (
    TracedDeadLetteringProjectionStore,
    TracedSqliteEventStore,
    configure_in_memory_tracing,
    summarize_spans,
)
from cqrs import CommandHandler


def test_hot_path_spans_carry_parentage_and_attributes() -> None:
    tracer, exporter = configure_in_memory_tracing("test-hot-path")
    store = TracedSqliteEventStore(":memory:", tracer)
    handler = CommandHandler(store)

    with tracer.start_as_current_span("command.handle"):
        handler.handle({"type": "Deposit", "account_id": "a", "amount": 10})
        handler.handle({"type": "Withdraw", "account_id": "a", "amount": 4})

    spans = {span.name: span for span in exporter.get_finished_spans()}
    assert set(spans) == {
        "command.handle",
        "eventstore.append",
        "eventstore.read_after",
    }
    root_context = spans["command.handle"].get_span_context()
    for child_name in ("eventstore.append", "eventstore.read_after"):
        parent = spans[child_name].parent
        assert parent is not None and parent.span_id == root_context.span_id
    assert spans["eventstore.append"].attributes["stream"] == "account-a"
    # The withdraw guard's catch-up read starts AFTER the deposit the memo
    # already folded at append time — direct evidence the memoization works.
    assert spans["eventstore.read_after"].attributes["after_seq"] == 1


def test_projection_apply_span_marks_duplicates() -> None:
    tracer, exporter = configure_in_memory_tracing("test-projection")
    projection = TracedDeadLetteringProjectionStore(":memory:", tracer)
    event = {
        "event_id": "evt-1",
        "id": 1,
        "type": "Deposited",
        "account_id": "a",
        "amount": 10,
    }
    assert projection.apply(dict(event)) is True
    assert projection.apply(dict(event)) is False

    applies = [
        span
        for span in exporter.get_finished_spans()
        if span.name == "projection.apply"
    ]
    assert [span.attributes["duplicate"] for span in applies] == [False, True]


def test_summarize_spans_aggregates_per_name() -> None:
    tracer, exporter = configure_in_memory_tracing("test-summary")
    for _ in range(3):
        with tracer.start_as_current_span("alpha"):
            pass
    with tracer.start_as_current_span("beta"):
        pass

    summary = summarize_spans(exporter)
    assert set(summary) == {"alpha", "beta"}
    assert summary["alpha"]["count"] == 3
    assert summary["beta"]["count"] == 1
    for stats in summary.values():
        assert 0 <= stats["mean_ms"] <= stats["max_ms"]
        assert stats["p95_ms"] <= stats["max_ms"]


def test_tracer_providers_are_isolated() -> None:
    tracer_one, exporter_one = configure_in_memory_tracing("iso-one")
    _tracer_two, exporter_two = configure_in_memory_tracing("iso-two")
    with tracer_one.start_as_current_span("only-in-one"):
        pass
    assert [span.name for span in exporter_one.get_finished_spans()] == ["only-in-one"]
    assert exporter_two.get_finished_spans() == ()
