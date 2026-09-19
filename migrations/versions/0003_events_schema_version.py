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
    # Dropping the column erases the only record of which shape each event was
    # written in; a later re-upgrade would relabel every row v1 and readers
    # would upcast v2 payloads as v1 -- the guess ADR-0009 forbids. Refuse
    # unless nothing would be lost (every row is v1). Fail closed (ADR-0007).
    op.execute(
        """
        DO $$
        DECLARE newer BIGINT;
        BEGIN
            SELECT COUNT(*) INTO newer FROM events WHERE schema_version <> 1;
            IF newer > 0 THEN
                RAISE EXCEPTION
                    'refusing to drop events.schema_version: % row(s) carry a '
                    'version other than 1; downgrading would lose the shape '
                    'they were written in', newer;
            END IF;
        END $$;
        """
    )
    op.execute("ALTER TABLE events DROP COLUMN IF EXISTS schema_version")
