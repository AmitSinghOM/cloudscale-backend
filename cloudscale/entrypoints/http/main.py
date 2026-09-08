"""Environment-configured app builder for real server runs.

Run with:

    CLOUDSCALE_JWT_SECRET=... CLOUDSCALE_LOG_DB=... CLOUDSCALE_PROJECTION_DB=... \\
        uvicorn --factory cloudscale.entrypoints.http.main:build_app

The command unit of work and the query projection share nothing in-process;
the projection is filled by the separate consumer loop process
(``python -m cloudscale.entrypoints.consumer_loop``) reading the same log.
"""

from __future__ import annotations

import os

from fastapi import FastAPI

from cloudscale.adapters.projection_readers import StoreProjectionReader
from cloudscale.adapters.sqlite_compat.command_unit_of_work import (
    SqliteCommandUnitOfWork,
)
from cloudscale.adapters.sqlite_compat.dead_letter_store import (
    DeadLetteringProjectionStore,
)
from cloudscale.application.command_service import CommandService
from cloudscale.application.query_service import QueryService
from cloudscale.entrypoints.http.app import create_app
from cloudscale.entrypoints.http.settings import HttpSettings


def build_app() -> FastAPI:
    log_db = os.environ["CLOUDSCALE_LOG_DB"]
    projection_db = os.environ["CLOUDSCALE_PROJECTION_DB"]
    settings = HttpSettings()
    unit_of_work = SqliteCommandUnitOfWork(log_db)
    projection = DeadLetteringProjectionStore(path=projection_db)
    return create_app(
        settings,
        command_service=CommandService(unit_of_work),
        query_service=QueryService(StoreProjectionReader(projection)),
        storage_metadata=projection.metadata,
    )


__all__ = ["build_app"]
