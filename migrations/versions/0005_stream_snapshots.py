"""Stream snapshots: a verified cache over the event log.

Revision ID: 0005_stream_snapshots
Revises: 0004_events_transfer_columns

ADR-0012. One row per stream: the fold of events with ``seq <= seq``, the
``AccountState`` shape version it was computed with, and the id of the event
at ``seq`` so a reader can verify the row against the log before trusting
it. Derived data only: the downgrade drops the table with nothing to guard,
because every row can be recomputed from ``events``.
"""

from __future__ import annotations

from alembic import op

revision = "0005_stream_snapshots"
down_revision = "0004_events_transfer_columns"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS stream_snapshots (
            stream          TEXT PRIMARY KEY,
            seq             BIGINT NOT NULL,
            state_json      TEXT NOT NULL,
            state_version   INTEGER NOT NULL,
            anchor_event_id TEXT NOT NULL
        )
        """
    )


def downgrade() -> None:
    # Snapshots are a cache (ADR-0012): dropping them loses nothing that the
    # event log cannot recompute, so this downgrade has no guard.
    op.execute("DROP TABLE IF EXISTS stream_snapshots")
