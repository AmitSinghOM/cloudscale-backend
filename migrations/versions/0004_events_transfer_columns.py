"""Double-entry transfers: pair the two legs of a transfer on the event row.

Revision ID: 0004_events_transfer_columns
Revises: 0003_events_schema_version

ADR-0011. ``TransferDebited`` / ``TransferCredited`` events carry the
``transfer_id`` that pairs them and the ``counterparty`` account. Both are
nullable: every other event type leaves them NULL. Metadata-only (no table
rewrite, no default).
"""

from __future__ import annotations

from alembic import op

revision = "0004_events_transfer_columns"
down_revision = "0003_events_schema_version"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS transfer_id TEXT")
    op.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS counterparty TEXT")


def downgrade() -> None:
    # Dropping the columns would leave transfer legs in the log with no record
    # of which credit pairs with which debit; a fold still balances, but the
    # double-entry audit trail is gone for good. Refuse unless nothing would
    # be lost. Fail closed (ADR-0007).
    op.execute(
        """
        DO $$
        DECLARE legs BIGINT;
        BEGIN
            SELECT COUNT(*) INTO legs FROM events
                WHERE type IN ('TransferDebited', 'TransferCredited');
            IF legs > 0 THEN
                RAISE EXCEPTION
                    'refusing to drop events.transfer_id/counterparty: % transfer '
                    'leg(s) exist; downgrading would lose their pairing', legs;
            END IF;
        END $$;
        """
    )
    op.execute("ALTER TABLE events DROP COLUMN IF EXISTS counterparty")
    op.execute("ALTER TABLE events DROP COLUMN IF EXISTS transfer_id")
