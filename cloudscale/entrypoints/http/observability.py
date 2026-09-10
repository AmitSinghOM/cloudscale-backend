"""Structured logging, audit trail, and Prometheus metrics for the HTTP tier.

- ``configure_logging()`` installs a JSON-lines formatter on the root logger
  (called by the real server entrypoint, never by ``create_app`` so tests
  keep pytest's capture).
- ``RequestLogMiddleware`` emits one ``http.request`` record per request
  with method, route, status, duration, and the caller subject when known.
- ``audit_command`` writes an append-only ``cloudscale.audit`` record for
  every command decision: who, which account, which command, what outcome.
  This is the record an incident or a disputed transaction is investigated
  from; it never contains the bearer token.
- ``Metrics`` holds a per-app ``CollectorRegistry`` (so tests can build many
  apps) with request and command-outcome counters and a latency histogram.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
)
from starlette.responses import Response

from cloudscale.domain.results import CommandResult

REQUEST_LOGGER = logging.getLogger("cloudscale.http")
AUDIT_LOGGER = logging.getLogger("cloudscale.audit")


class JsonFormatter(logging.Formatter):
    """One JSON object per line; structured fields come from ``extra``."""

    _RESERVED = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {
        "message",
        "asctime",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in self._RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, sort_keys=True)


def configure_logging(level: int = logging.INFO) -> None:
    """Install JSON-lines logging on the root logger (idempotent)."""
    root = logging.getLogger()
    for handler in root.handlers:
        if getattr(handler, "_cloudscale_json", False):
            return
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    handler._cloudscale_json = True  # type: ignore[attr-defined]
    root.addHandler(handler)
    root.setLevel(level)


class RequestLogMiddleware:
    """Structured access log with latency; records the subject when set.

    Pure ASGI middleware. ``BaseHTTPMiddleware`` re-streams every response
    through a task group and measurably cut throughput (1,431 -> 968 rps in
    the gate harness); this observes ``http.response.start`` instead and
    adds no buffering.
    """

    def __init__(self, app: Any, metrics: Metrics) -> None:
        self._app = app
        self._metrics = metrics

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        started = time.perf_counter()
        status_holder = {"status": 500}

        async def send_wrapper(message: dict) -> None:
            if message["type"] == "http.response.start":
                status_holder["status"] = message["status"]
            await send(message)

        try:
            await self._app(scope, receive, send_wrapper)
        finally:
            duration = time.perf_counter() - started
            route = scope.get("route")
            path_template = getattr(route, "path", scope.get("path", ""))
            status = status_holder["status"]
            self._metrics.observe_request(
                scope["method"], path_template, status, duration
            )
            state = scope.get("state") or {}
            client = scope.get("client")
            REQUEST_LOGGER.info(
                "http.request",
                extra={
                    "method": scope["method"],
                    "route": path_template,
                    "status": status,
                    "duration_ms": round(duration * 1000, 3),
                    "subject": state.get("subject"),
                    "client": client[0] if client else None,
                },
            )


def audit_command(
    *,
    subject: str,
    issuer: str,
    account_id: str,
    command_type: str,
    command_id: UUID,
    result: CommandResult,
) -> None:
    """Append-only audit record for one command decision."""
    AUDIT_LOGGER.info(
        "command.decided",
        extra={
            "subject": subject,
            "issuer": issuer,
            "account_id": account_id,
            "command_type": command_type,
            "command_id": str(command_id),
            "correlation_id": str(result.correlation_id),
            "outcome": result.outcome.value,
            "http_status": result.http_status,
            "committed_version": result.committed_version,
            "event_id": str(result.event_id) if result.event_id else None,
            "error_code": result.error_code,
        },
    )


class Metrics:
    """Per-app Prometheus registry and the instruments the tier exposes."""

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self._requests = Counter(
            "cloudscale_http_requests_total",
            "HTTP requests by method, route, status.",
            ("method", "route", "status"),
            registry=self.registry,
        )
        self._latency = Histogram(
            "cloudscale_http_request_seconds",
            "HTTP request latency.",
            ("method", "route"),
            registry=self.registry,
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
        )
        self._commands = Counter(
            "cloudscale_command_outcomes_total",
            "Command decisions by outcome.",
            ("outcome",),
            registry=self.registry,
        )

    def observe_request(
        self, method: str, route: str, status: int, duration_seconds: float
    ) -> None:
        self._requests.labels(method, route, str(status)).inc()
        self._latency.labels(method, route).observe(duration_seconds)

    def observe_command(self, result: CommandResult) -> None:
        self._commands.labels(result.outcome.value).inc()

    def render(self) -> Response:
        return Response(generate_latest(self.registry), media_type=CONTENT_TYPE_LATEST)


__all__ = [
    "AUDIT_LOGGER",
    "JsonFormatter",
    "Metrics",
    "REQUEST_LOGGER",
    "RequestLogMiddleware",
    "audit_command",
    "configure_logging",
]
