"""Initial schema: everything the PostgreSQL tier needs as of v0.4.x.

Revision ID: 0001_initial
Revises: None

Runs the exact DDL the adapters use in ``auto`` mode (single source in
``cloudscale.adapters.postgres.schema``), so a database created either way
is identical — asserted by ``tests/unit/adapters/test_postgres_migrations.py``.
"""

from __future__ import annotations

from alembic import op

from cloudscale.adapters.postgres import schema

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statements in schema.ALL:
        op.execute(statements)


def downgrade() -> None:
    # Reverse dependency order; outbox references events.
    for table in (
        "rate_limit_buckets",
        "accounts",
        "dead_letters",
        "consumer_offset",
        "processed_events",
        "balances",
        "event_envelopes",
        "command_results",
        "outbox",
        "events",
    ):
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
