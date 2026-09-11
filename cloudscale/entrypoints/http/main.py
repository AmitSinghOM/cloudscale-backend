"""Environment-configured app builder for real server runs.

Run with:

    CLOUDSCALE_JWT_SECRET=... CLOUDSCALE_LOG_DB=... CLOUDSCALE_PROJECTION_DB=... \\
        uvicorn --factory cloudscale.entrypoints.http.main:build_app

Storage tier selection (``CLOUDSCALE_STORAGE``):

- ``sqlite`` (default): ``CLOUDSCALE_LOG_DB`` + ``CLOUDSCALE_PROJECTION_DB``
  file paths.
- ``postgres``: ``CLOUDSCALE_PG_DSN`` — the unit of work and the projection
  share the database; transient-error classification switches to
  ``psycopg.OperationalError`` for the command path's breaker/retry.

The projection is filled by the separate consumer loop process
(``python -m cloudscale.entrypoints.consumer_loop``) reading the same log.
"""

from __future__ import annotations

import os

from fastapi import FastAPI

from cloudscale.adapters.projection_readers import StoreProjectionReader
from cloudscale.application.command_service import CommandService
from cloudscale.application.query_service import QueryService
from cloudscale.entrypoints.http.app import create_app
from cloudscale.entrypoints.http.observability import configure_logging
from cloudscale.entrypoints.http.settings import HttpSettings


def build_app() -> FastAPI:
    configure_logging()
    storage = os.environ.get("CLOUDSCALE_STORAGE", "sqlite")
    # jwt_secret arrives via CLOUDSCALE_JWT_SECRET; pydantic-settings raises
    # at startup when absent (fail-closed by design).
    settings = HttpSettings()  # type: ignore[call-arg]

    if storage == "postgres":
        import psycopg

        from cloudscale.adapters.postgres.account_registry import (
            PostgresAccountRegistry,
        )
        from cloudscale.adapters.postgres.command_unit_of_work import (
            PostgresCommandUnitOfWork,
        )
        from cloudscale.adapters.postgres.projection_store import (
            PostgresProjectionStore,
        )

        dsn = os.environ["CLOUDSCALE_PG_DSN"]
        pg_unit_of_work = PostgresCommandUnitOfWork(dsn)
        pg_projection = PostgresProjectionStore(dsn)
        pg_registry = PostgresAccountRegistry(dsn)
        return create_app(
            settings,
            command_service=CommandService(pg_unit_of_work),
            query_service=QueryService(StoreProjectionReader(pg_projection)),
            storage_metadata=pg_projection.metadata,
            transient_errors=(psycopg.OperationalError,),
            account_registry=pg_registry,
            closeables=(pg_unit_of_work, pg_projection, pg_registry),
        )

    if storage != "sqlite":
        raise ValueError(f"unsupported CLOUDSCALE_STORAGE: {storage!r}")

    from cloudscale.adapters.sqlite_compat.account_registry import (
        SqliteAccountRegistry,
    )
    from cloudscale.adapters.sqlite_compat.command_unit_of_work import (
        SqliteCommandUnitOfWork,
    )
    from cloudscale.adapters.sqlite_compat.dead_letter_store import (
        DeadLetteringProjectionStore,
    )

    log_db = os.environ["CLOUDSCALE_LOG_DB"]
    unit_of_work = SqliteCommandUnitOfWork(log_db)
    projection = DeadLetteringProjectionStore(
        path=os.environ["CLOUDSCALE_PROJECTION_DB"]
    )
    registry = SqliteAccountRegistry(log_db)  # ownership lives beside the log
    return create_app(
        settings,
        command_service=CommandService(unit_of_work),
        query_service=QueryService(StoreProjectionReader(projection)),
        storage_metadata=projection.metadata,
        account_registry=registry,
        closeables=(unit_of_work, projection, registry),
    )


__all__ = ["build_app"]
