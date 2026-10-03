"""Shared harness for restored HTTP replay tests (P1-C).

A ``Tier`` wraps the restore drill's own SQLite or PostgreSQL tier, so dump,
restore and the authoritative-table snapshot are exactly what the drill uses.
``Sent`` keeps a request as the source saw it (path, body, issuer, subject,
claims, original response); a replay mints a fresh token from those fields.
"""

from __future__ import annotations

import os
import shutil
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import jwt
from fastapi.testclient import TestClient

from cloudscale.adapters.projection_readers import StoreProjectionReader
from cloudscale.application.command_service import CommandService
from cloudscale.application.query_service import QueryService
from cloudscale.entrypoints.http.app import create_app
from cloudscale.entrypoints.http.settings import HttpSettings
from scripts.restore_drill import (
    PostgresDrill,
    Report,
    SqliteDrill,
    _pg_client_major,
    _resolve_pg_bin,
)

SECRET = "restored-replay-secret-0123456789abcdef-0123456789abcdef"
ADMIN_DSN = os.environ.get("CLOUDSCALE_TEST_PG")


def postgres_skip_reason() -> str | None:
    """Only an explicitly supplied server is used; there is no default DSN."""
    if not ADMIN_DSN:
        return "CLOUDSCALE_TEST_PG is not set"
    try:
        import psycopg
    except ImportError:
        return "psycopg is not installed"
    try:
        with psycopg.connect(ADMIN_DSN, connect_timeout=2) as conn:
            row = conn.execute(
                "SELECT current_setting('server_version_num')"
            ).fetchone()
    except psycopg.OperationalError:
        return "PostgreSQL at CLOUDSCALE_TEST_PG is unreachable"
    server = None if row is None else int(row[0]) // 10_000
    pg_bin = _resolve_pg_bin(None)
    tool = str(pg_bin / "pg_dump") if pg_bin else shutil.which("pg_dump")
    if not tool or not Path(tool).exists():
        return "pg_dump not found (set CLOUDSCALE_PG_BIN)"
    client = _pg_client_major(tool)
    if client != server:
        return f"pg_dump is {client}, server is {server} (set CLOUDSCALE_PG_BIN)"
    return None


@dataclass(frozen=True)
class Identity:
    subject: str
    accounts: tuple[str, ...] | None = None
    issuer: str = "cloudscale"

    def headers(self) -> dict[str, str]:
        now = datetime.now(UTC)
        claims: dict[str, Any] = {
            "iss": self.issuer,
            "sub": self.subject,
            "iat": now,
            "exp": now + timedelta(minutes=5),
        }
        if self.accounts is not None:
            claims["accounts"] = list(self.accounts)
        token = jwt.encode(claims, SECRET, algorithm="HS256")
        return {"Authorization": f"Bearer {token}"}


@dataclass(frozen=True)
class Sent:
    path: str
    body: dict[str, Any]
    identity: Identity
    status: int
    response: dict[str, Any]

    @property
    def command_id(self) -> uuid.UUID:
        return uuid.UUID(self.body["command_id"])


@dataclass
class Store:
    """An HTTP app over one database plus its adapters, for direct reads."""

    client: TestClient
    uow: Any
    registry: Any

    def send(self, path: str, body: dict[str, Any], identity: Identity) -> Sent:
        response = self.client.post(path, json=body, headers=identity.headers())
        return Sent(path, body, identity, response.status_code, response.json())

    def replay(self, sent: Sent, identity: Identity | None = None) -> Any:
        return self.client.post(
            sent.path, json=sent.body, headers=(identity or sent.identity).headers()
        )

    def states(self, accounts: tuple[str, ...]) -> dict[str, Any]:
        return {account: self.uow.fold_stream(account) for account in accounts}


def command(amount: int, expected_version: int, kind: str = "deposit") -> dict:
    return {
        "command_id": str(uuid.uuid4()),
        "type": kind,
        "amount": amount,
        "expected_version": expected_version,
    }


def postings(legs: list[tuple[str, int, str]], version: int) -> dict:
    return {
        "command_id": str(uuid.uuid4()),
        "expected_version": version,
        "postings": [
            {"account_id": a, "amount": n, "direction": d} for a, n, d in legs
        ],
    }


class Tier:
    """One storage tier: fresh source, real dump/restore, apps over either copy."""

    def __init__(self, name: str, workdir: Path) -> None:
        from cloudscale.adapters.postgres import command_unit_of_work as pg_uow
        from cloudscale.adapters.sqlite_compat import command_unit_of_work as sq_uow

        self.name = name
        report = Report(tier=name, mode="seeded")
        self.drill: SqliteDrill | PostgresDrill
        if name == "sqlite":
            self.drill = SqliteDrill(
                workdir, consumer="balances", keep=False, report=report
            )
            self.uow_class: type = sq_uow.SqliteCommandUnitOfWork
            self.decision_module: Any = sq_uow
        else:
            assert ADMIN_DSN is not None
            self.drill = PostgresDrill(
                ADMIN_DSN,
                pg_bin=_resolve_pg_bin(None),
                consumer="balances",
                keep=False,
                workdir=workdir,
                report=report,
            )
            self.uow_class = pg_uow.PostgresCommandUnitOfWork
            self.decision_module = pg_uow

    @contextmanager
    def active(self) -> Iterator[Tier]:
        with self.drill.environment():
            try:
                yield self
            finally:
                self.drill.cleanup()  # drops only what this tier created

    def fresh_source(self) -> str:
        source = self.drill.create_database("source")
        self.drill.migrate_head(source)
        return source

    def dump_and_restore(self, source: str) -> str:
        restored = self.drill.restore(self.drill.dump(source))
        started, detail = self.drill.adapters_start(restored)
        assert started, detail
        return restored

    def snapshot(self, dsn: str) -> dict[str, Any]:
        return self.drill.authoritative_snapshot(dsn)

    @contextmanager
    def store(self, dsn: str, pool_max: int = 2) -> Iterator[Store]:
        if self.name == "sqlite":
            from cloudscale.adapters.sqlite_compat.account_registry import (
                SqliteAccountRegistry,
            )
            from cloudscale.adapters.sqlite_compat.command_unit_of_work import (
                SqliteCommandUnitOfWork,
            )
            from cloudscale.adapters.sqlite_compat.dead_letter_store import (
                DeadLetteringProjectionStore,
            )

            log = str(Path(dsn) / "log.db")
            uow: Any = SqliteCommandUnitOfWork(log)
            projection: Any = DeadLetteringProjectionStore(
                path=str(Path(dsn) / "projection.db")
            )
            registry: Any = SqliteAccountRegistry(log)
            extra: dict[str, Any] = {}
        else:
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

            uow = PostgresCommandUnitOfWork(dsn, pool_max=pool_max)
            projection = PostgresProjectionStore(dsn, pool_max=1)
            registry = PostgresAccountRegistry(dsn, pool_max=pool_max)
            extra = {"transient_errors": (psycopg.OperationalError,)}
        app = create_app(
            HttpSettings(jwt_secret=SECRET, rate_limit_per_minute=0),
            command_service=CommandService(uow),
            query_service=QueryService(StoreProjectionReader(projection)),
            storage_metadata=projection.metadata,
            account_registry=registry,
            closeables=(uow, projection, registry),
            **extra,
        )
        with TestClient(app) as client:
            yield Store(client=client, uow=uow, registry=registry)
