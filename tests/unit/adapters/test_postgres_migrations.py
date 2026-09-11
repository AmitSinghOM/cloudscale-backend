"""Alembic migrations and the shared rate limiter, against live PostgreSQL.

Skipped when no server is reachable at ``CLOUDSCALE_TEST_PG``; every
database created here is dropped in teardown.
"""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Iterator

import pytest

psycopg = pytest.importorskip("psycopg")

from alembic import command  # noqa: E402

from cloudscale.adapters.postgres import schema  # noqa: E402
from cloudscale.adapters.postgres.pool import (  # noqa: E402
    SchemaNotMigratedError,
    open_pool,
)
from cloudscale.adapters.postgres.projection_store import (  # noqa: E402
    PostgresProjectionStore,
)
from cloudscale.adapters.postgres.rate_limiter import PostgresRateLimiter  # noqa: E402
from cloudscale.entrypoints.migrate import alembic_config  # noqa: E402

_ADMIN_DSN = os.environ.get("CLOUDSCALE_TEST_PG", "postgresql://localhost/postgres")


def _postgres_available() -> bool:
    try:
        with psycopg.connect(_ADMIN_DSN, connect_timeout=2):
            return True
    except psycopg.OperationalError:
        return False


pytestmark = pytest.mark.skipif(
    not _postgres_available(), reason=f"no PostgreSQL reachable at {_ADMIN_DSN}"
)


@pytest.fixture()
def fresh_dsn() -> Iterator[str]:
    """A brand-new empty database per test, dropped afterwards."""
    database = f"cloudscale_mig_{uuid.uuid4().hex[:12]}"
    admin = psycopg.connect(_ADMIN_DSN, autocommit=True)
    admin.execute(f'CREATE DATABASE "{database}"')
    try:
        yield f"{_ADMIN_DSN.rsplit('/', 1)[0]}/{database}"
    finally:
        admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
        admin.close()


def _describe(dsn: str) -> dict:
    """Schema fingerprint: columns and indexes for every public table."""
    with psycopg.connect(dsn) as conn:
        columns = conn.execute(
            "SELECT table_name, column_name, data_type, is_nullable, "
            "column_default IS NOT NULL AS has_default "
            "FROM information_schema.columns WHERE table_schema = 'public' "
            "AND table_name <> 'alembic_version' "
            "ORDER BY table_name, ordinal_position"
        ).fetchall()
        indexes = conn.execute(
            "SELECT tablename, indexdef FROM pg_indexes WHERE schemaname = 'public' "
            "AND tablename <> 'alembic_version' ORDER BY tablename, indexname"
        ).fetchall()
    return {"columns": columns, "indexes": indexes}


def _upgrade(dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLOUDSCALE_PG_DSN", dsn)
    command.upgrade(alembic_config(), "head")


# -- migrations -------------------------------------------------------------------


def test_upgrade_head_creates_the_full_schema_and_stamps_revision(
    fresh_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    _upgrade(fresh_dsn, monkeypatch)
    with psycopg.connect(fresh_dsn) as conn:
        version = conn.execute("SELECT version_num FROM alembic_version").fetchone()
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public'"
            ).fetchall()
        }
    assert version is not None and version[0] == schema.CURRENT_REVISION
    assert {
        "events",
        "outbox",
        "command_results",
        "event_envelopes",
        "balances",
        "processed_events",
        "consumer_offset",
        "dead_letters",
        "accounts",
        "rate_limit_buckets",
    } <= tables


def test_migrated_schema_is_identical_to_auto_created_schema(
    fresh_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The single-source guarantee, asserted rather than assumed."""
    _upgrade(fresh_dsn, monkeypatch)
    migrated = _describe(fresh_dsn)

    # Build a second database the adapters' way (auto mode).
    database = f"cloudscale_auto_{uuid.uuid4().hex[:12]}"
    admin = psycopg.connect(_ADMIN_DSN, autocommit=True)
    admin.execute(f'CREATE DATABASE "{database}"')
    auto_dsn = f"{_ADMIN_DSN.rsplit('/', 1)[0]}/{database}"
    try:
        monkeypatch.setenv("CLOUDSCALE_PG_SCHEMA", "auto")
        pool = open_pool(auto_dsn, max_size=1)
        with pool.connection() as conn, conn.transaction():
            for statements in schema.ALL:
                conn.execute(statements)
        pool.close()
        auto = _describe(auto_dsn)
    finally:
        admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
        admin.close()

    assert migrated == auto


def test_migrations_mode_refuses_an_unmigrated_database(
    fresh_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLOUDSCALE_PG_SCHEMA", "migrations")
    with pytest.raises(SchemaNotMigratedError, match="no alembic_version"):
        PostgresProjectionStore(fresh_dsn)
    # Nothing was created behind our back.
    with psycopg.connect(fresh_dsn) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'"
        ).fetchone()
    assert count is not None and count[0] == 0


def test_migrations_mode_starts_once_migrated_and_never_creates(
    fresh_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    _upgrade(fresh_dsn, monkeypatch)
    monkeypatch.setenv("CLOUDSCALE_PG_SCHEMA", "migrations")
    store = PostgresProjectionStore(fresh_dsn)
    try:
        assert store.last_id() == 0
    finally:
        store.close()


def test_migrations_mode_refuses_a_wrong_revision(
    fresh_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    _upgrade(fresh_dsn, monkeypatch)
    with psycopg.connect(fresh_dsn, autocommit=True) as conn:
        conn.execute("UPDATE alembic_version SET version_num = '9999_future'")
    monkeypatch.setenv("CLOUDSCALE_PG_SCHEMA", "migrations")
    with pytest.raises(SchemaNotMigratedError, match="9999_future"):
        PostgresProjectionStore(fresh_dsn)


# -- shared rate limiter ------------------------------------------------------------------


def test_replicas_share_one_budget_and_refill_uses_server_clock(
    fresh_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLOUDSCALE_PG_SCHEMA", "auto")
    replica_a = PostgresRateLimiter(fresh_dsn, per_minute=60, pool_max=1)  # 1 token/s
    replica_b = PostgresRateLimiter(fresh_dsn, per_minute=60, pool_max=1)
    try:
        # Capacity 60: replica A takes 59, replica B takes the 60th; the
        # budget is shared, so B's next request is refused.
        for _ in range(59):
            assert replica_a.try_acquire("alice")[0] is True
        assert replica_b.try_acquire("alice")[0] is True
        denied, retry_after = replica_b.try_acquire("alice")
        assert denied is False
        assert 0.0 < retry_after <= 1.0
        # A different subject has its own bucket.
        assert replica_a.try_acquire("bob")[0] is True
        # Server-clock refill: after ~1s one token is back for either replica.
        time.sleep(1.1)
        assert replica_a.try_acquire("alice")[0] is True
    finally:
        replica_a.close()
        replica_b.close()


def test_shared_limiter_disabled_at_zero(
    fresh_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLOUDSCALE_PG_SCHEMA", "auto")
    limiter = PostgresRateLimiter(fresh_dsn, per_minute=0, pool_max=1)
    try:
        assert limiter.enabled is False
        assert limiter.try_acquire("anyone") == (True, 0.0)
    finally:
        limiter.close()
