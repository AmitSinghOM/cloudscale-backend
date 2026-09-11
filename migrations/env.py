"""Alembic environment: URL from CLOUDSCALE_PG_DSN, raw-DDL migrations.

Migrations are hand-written DDL that mirrors the adapters' schema exactly
(a PG-gated test asserts parity), so there is no SQLAlchemy metadata and no
autogenerate.
"""

from __future__ import annotations

import os

from alembic import context
from sqlalchemy import create_engine


def _url() -> str:
    dsn = os.environ.get("CLOUDSCALE_PG_DSN")
    if not dsn:
        raise RuntimeError("CLOUDSCALE_PG_DSN must be set to run migrations")
    # psycopg (v3) driver for SQLAlchemy.
    if dsn.startswith("postgresql://"):
        return "postgresql+psycopg://" + dsn[len("postgresql://") :]
    return dsn


def run_migrations_offline() -> None:
    context.configure(url=_url(), literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(_url())
    with engine.connect() as connection:
        context.configure(connection=connection)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
