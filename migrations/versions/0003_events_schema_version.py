"""Event schema evolution: record the schema version on every event row.

Revision ID: 0003_events_schema_version
Revises: 0002_command_results_created_at

ADR-0009. Readers upcast events to the current shape at read time; to do
that they need the version the row was written with, on the row itself
(``event_envelopes`` holds it too, but the legacy append path does not
write envelopes). Existing rows default to 1 — by definition the first
shape ever written. Metadata-only on PostgreSQL 11+ (no table rewrite).
"""

from __future__ import annotations

from alembic import op

revision = "0003_events_schema_version"
down_revision = "0002_command_results_created_at"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE events ADD COLUMN IF NOT EXISTS "
        "schema_version INTEGER NOT NULL DEFAULT 1"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE events DROP COLUMN IF EXISTS schema_version")
