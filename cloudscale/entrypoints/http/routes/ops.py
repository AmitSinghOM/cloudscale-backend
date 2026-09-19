"""Liveness, readiness and metrics: the routes an orchestrator talks to."""

from __future__ import annotations

import logging

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from cloudscale.entrypoints.http.context import API_VERSION, RouteContext
from cloudscale.entrypoints.http.models import (
    HealthResponse,
    ReadinessProbe,
    ReadyResponse,
)

_LOGGER = logging.getLogger("cloudscale.http")


def build_router(
    ctx: RouteContext,
    *,
    storage_metadata: dict[str, object],
    readiness_probe: ReadinessProbe | None,
) -> APIRouter:
    router = APIRouter()

    @router.get(f"/{API_VERSION}/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        # Liveness only: the process is up. Never touches storage, so it
        # cannot be used to decide whether to route traffic here.
        return HealthResponse(status="ok", storage=storage_metadata)

    @router.get(
        f"/{API_VERSION}/ready",
        response_model=ReadyResponse,
        responses={503: {"model": ReadyResponse}},
    )
    def ready() -> JSONResponse:
        # Readiness: storage round-trip (and schema revision on the PG tier).
        # 503 tells the orchestrator to stop routing to this replica and to
        # fail a rollout of a build that cannot reach or does not match the
        # database. Without a probe wired, readiness == liveness (SQLite dev).
        if readiness_probe is None:
            return JSONResponse(
                ReadyResponse(status="ready", checks={"probe": "none"}).model_dump()
            )
        try:
            checks = readiness_probe.check()
        except Exception as exc:  # noqa: BLE001 — any failure means not ready
            # Unauthenticated endpoint: expose only the exception class. The
            # message can carry host names or DSN fragments; that goes to the
            # log, where operators (not the internet) read it.
            _LOGGER.warning(
                "readiness probe failed",
                extra={"error_type": type(exc).__name__, "error": str(exc)[:500]},
            )
            body = ReadyResponse(
                status="not_ready", checks={"error": type(exc).__name__}
            )
            return JSONResponse(body.model_dump(), status_code=503)
        return JSONResponse(ReadyResponse(status="ready", checks=checks).model_dump())

    @router.get("/metrics", include_in_schema=False)
    def prometheus_metrics() -> object:
        return ctx.metrics.render()

    return router


__all__ = ["build_router"]
