"""Reverts: link a reversal to the posting set it mirrors.

Revision ID: 0007_reverts
Revises: 0006_holds

ADR-0015. ``ReversalDebited`` / ``ReversalCredited`` carry ``reverts`` (the
mirrored set's ``transfer_id``); nullable elsewhere. Two indexes serve the
in-transaction derivations: ``events_transfer_idx`` on ``transfer_id`` alone
(``legs_of`` reads a set across streams; the existing index is per stream)
and a partial ``events_reverts_idx`` (``reverted_by``).
"""

from __future__ import annotations

from alembic import op

revision = "0007_reverts"
down_revision = "0006_holds"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS reverts TEXT")
    op.execute(
        "CREATE INDEX IF NOT EXISTS events_transfer_idx ON events (transfer_id) "
        "WHERE transfer_id IS NOT NULL"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS events_reverts_idx ON events (reverts) "
        "WHERE reverts IS NOT NULL"
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS transfers (
            transfer_id TEXT PRIMARY KEY,
            kind        TEXT NOT NULL,
            reverts     TEXT,
            reverted_by TEXT
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS transfer_legs (
            transfer_id TEXT NOT NULL,
            account_id  TEXT NOT NULL,
            amount      BIGINT NOT NULL,
            direction   TEXT NOT NULL,
            PRIMARY KEY (transfer_id, account_id)
        )
        """
    )


def downgrade() -> None:
    # A reversal without its ``reverts`` link is a posting set that no longer
    # says what it undid, and a second revert of the original would then be
    # accepted. Refuse while any reversal exists. Fail closed (ADR-0007).
    op.execute(
        """
        DO $$
        DECLARE reversals BIGINT;
        BEGIN
            SELECT COUNT(*) INTO reversals FROM events
                WHERE type IN ('ReversalDebited', 'ReversalCredited');
            IF reversals > 0 THEN
                RAISE EXCEPTION
                    'refusing to drop events.reverts: % reversal leg(s) exist; '
                    'downgrading would lose what they undo', reversals;
            END IF;
        END $$;
        """
    )
    # Read-model rows are derived: removing them loses nothing the log cannot rebuild.
    op.drop_table("transfer_legs")
    op.drop_table("transfers")
    op.execute("DROP INDEX IF EXISTS events_reverts_idx")
    op.execute("DROP INDEX IF EXISTS events_transfer_idx")
    op.execute("ALTER TABLE events DROP COLUMN IF EXISTS reverts")
