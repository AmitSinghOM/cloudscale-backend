"""Retention: timestamp idempotency records so they can be aged out.

Revision ID: 0002_command_results_created_at
Revises: 0001_initial

``command_results`` had no notion of time, so it could only grow. Adding
``created_at`` (server clock, defaulted so existing rows are stamped at
migration time — the safest assumption: treat them as fresh) lets the
retention job prune records older than the idempotency window. Indexed
because the job deletes by range in batches.

Online: ``ADD COLUMN ... DEFAULT now()`` is metadata-only on PostgreSQL 11+
(no table rewrite); ``CREATE INDEX`` takes a SHARE lock — run at low
traffic or swap for ``CONCURRENTLY`` outside a transaction on very large
tables.
"""

from __future__ import annotations

from alembic import op

revision = "0002_command_results_created_at"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE command_results "
        "ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS command_results_created_at_idx "
        "ON command_results (created_at)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS command_results_created_at_idx")
    op.execute("ALTER TABLE command_results DROP COLUMN IF EXISTS created_at")
