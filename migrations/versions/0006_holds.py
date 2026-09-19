"""Pending transfers (holds): event columns, held balance, holds read model.

Revision ID: 0006_holds
Revises: 0005_stream_snapshots

ADR-0014. ``HoldPlaced`` carries ``expires_at`` and ``HoldReleased`` a
``release_reason``; both ride nullable columns beside the transfer columns
(a hold's id is stored in ``transfer_id``: it becomes the transfer id of the
eventual posting). ``balances.held`` is the projected sum of open holds and
``holds`` is the per-hold read model the sweeper reads. A partial index on
``(stream, transfer_id)`` serves the open-hold derivation inside the command
transaction.
"""

from __future__ import annotations

from alembic import op

revision = "0006_holds"
down_revision = "0005_stream_snapshots"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS expires_at TEXT")
    op.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS release_reason TEXT")
    op.execute(
        "CREATE INDEX IF NOT EXISTS events_stream_transfer_idx ON events "
        "(stream, transfer_id) WHERE transfer_id IS NOT NULL"
    )
    op.execute(
        "ALTER TABLE balances ADD COLUMN IF NOT EXISTS held BIGINT NOT NULL DEFAULT 0"
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS holds (
            hold_id    TEXT PRIMARY KEY,
            source     TEXT NOT NULL,
            target     TEXT NOT NULL,
            amount     BIGINT NOT NULL,
            expires_at TEXT NOT NULL,
            state      TEXT NOT NULL
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS holds_open_expiry_idx ON holds (expires_at) "
        "WHERE state = 'open'"
    )


def downgrade() -> None:
    # Dropping expires_at/release_reason would leave hold events in the log
    # unable to fold (a HoldReleased needs its reason; a HoldPlaced its
    # expiry). Refuse while any hold event exists. Fail closed (ADR-0007).
    op.execute(
        """
        DO $$
        DECLARE holds_count BIGINT;
        BEGIN
            SELECT COUNT(*) INTO holds_count FROM events
                WHERE type IN ('HoldPlaced', 'HoldReleased', 'HoldPosted');
            IF holds_count > 0 THEN
                RAISE EXCEPTION
                    'refusing to drop hold columns: % hold event(s) exist; '
                    'downgrading would make them unfoldable', holds_count;
            END IF;
        END $$;
        """
    )
    op.execute("DROP INDEX IF EXISTS holds_open_expiry_idx")
    op.execute("DROP TABLE IF EXISTS holds")
    op.execute("ALTER TABLE balances DROP COLUMN IF EXISTS held")
    op.execute("DROP INDEX IF EXISTS events_stream_transfer_idx")
    op.execute("ALTER TABLE events DROP COLUMN IF EXISTS release_reason")
    op.execute("ALTER TABLE events DROP COLUMN IF EXISTS expires_at")
