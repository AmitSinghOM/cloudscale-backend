"""PostgreSQL event store: pooled connections and a transactional outbox.

Same append contract as ``SqliteEventStore`` (per-stream 1-based ``seq``,
optimistic concurrency via UNIQUE(stream, seq), stable ``event_id``).

Why an outbox
=============
``events.id`` is an IDENTITY column. Under concurrent writers a *smaller* id
can commit *after* a larger one has already been consumed, and a consumer
keeping a monotonic ``id`` offset would skip it forever. The fix is to give
consumers a different, gapless, commit-ordered coordinate:

- ``append`` writes the event row only.
- ``relay_outbox`` runs under an advisory lock, selects every committed
  event not yet in ``outbox`` (an anti-join, so late-committing ids are
  picked up), and assigns them consecutive ``position`` values in id order.
  Positions are gapless and reflect commit visibility, never assignment.
- ``read_all`` relays first, then reads by ``position``; the ``id`` key on
  returned events IS the outbox position, so the existing consumer offset
  logic works unchanged.

The relay is idempotent and cheap when there is nothing to publish; multiple
consumers serialize on the lock rather than double-assigning.
"""

from __future__ import annotations

import uuid

import psycopg

from cloudscale.adapters.postgres import schema
from cloudscale.adapters.postgres.pool import ensure_schema, open_pool
from cqrs import ConcurrencyError

_SCHEMA = schema.EVENTS + schema.OUTBOX

_RELAY_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtext('cloudscale_outbox_relay'))"


class PostgresEventStore:
    """Durable, append-only event log on PostgreSQL with an outbox feed."""

    def __init__(self, conninfo: str, *, pool_max: int | None = None) -> None:
        self._pool = open_pool(conninfo, max_size=pool_max)
        ensure_schema(self._pool, _SCHEMA)

    def close(self) -> None:
        self._pool.close()

    # -- write path ----------------------------------------------------------

    def append(self, stream: str, event: dict) -> int:
        """Append ``event`` to ``stream``; return its per-stream ``seq``."""
        if not isinstance(stream, str) or not stream:
            raise ValueError("stream must be a non-empty string")
        if not isinstance(event, dict):
            raise TypeError("event must be a dict")

        event_id = event.get("event_id") or str(uuid.uuid4())
        with self._pool.connection() as conn:
            try:
                with conn.transaction():
                    row = conn.execute(
                        "SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq "
                        "FROM events WHERE stream = %s",
                        (stream,),
                    ).fetchone()
                    assert row is not None
                    seq = int(row["next_seq"])
                    conn.execute(
                        "INSERT INTO events "
                        "(event_id, stream, seq, type, account_id, amount) "
                        "VALUES (%s, %s, %s, %s, %s, %s)",
                        (
                            event_id,
                            stream,
                            seq,
                            event.get("type"),
                            event.get("account_id"),
                            event.get("amount"),
                        ),
                    )
            except psycopg.errors.UniqueViolation as exc:
                raise ConcurrencyError(str(exc)) from exc
            return seq

    # -- outbox relay ----------------------------------------------------------

    def relay_outbox(self) -> int:
        """Publish every committed-but-unpublished event; return how many.

        Index scan over the partial ``events_unpublished_idx``; position
        assignment and the ``published`` flip commit together so a crash
        mid-relay republishes nothing and skips nothing.
        """
        with self._pool.connection() as conn, conn.transaction():
            conn.execute(_RELAY_LOCK_SQL)
            pending = conn.execute(
                "SELECT id, event_id FROM events WHERE NOT published ORDER BY id ASC"
            ).fetchall()
            if not pending:
                return 0
            head = conn.execute(
                "SELECT COALESCE(MAX(position), 0) AS head FROM outbox"
            ).fetchone()
            assert head is not None
            position = int(head["head"])
            outbox_rows = []
            for row in pending:
                position += 1
                outbox_rows.append((position, row["event_id"]))
            with conn.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO outbox (position, event_id) VALUES (%s, %s)",
                    outbox_rows,
                )
            conn.execute(
                "UPDATE events SET published = true WHERE id = ANY(%s)",
                ([int(row["id"]) for row in pending],),
            )
            return len(outbox_rows)

    # -- read path -------------------------------------------------------------

    def read(self, stream: str) -> list[dict]:
        return self.read_after(stream, 0)

    def read_after(self, stream: str, after_seq: int) -> list[dict]:
        if after_seq < 0:
            raise ValueError("after_seq must be non-negative")
        with self._pool.connection() as conn:
            rows = conn.execute(
                "SELECT event_id, stream, seq, type, account_id, amount "
                "FROM events WHERE stream = %s AND seq > %s ORDER BY seq ASC",
                (stream, after_seq),
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def read_all(self, after_id: int = 0, limit: int | None = None) -> list[dict]:
        """Consumer feed by outbox ``position`` (returned as ``id``).

        Relays first so newly committed events - including late-committing
        smaller ids - are published before being read. Gapless and
        commit-ordered; safe for a monotonic consumer offset.
        """
        self.relay_outbox()
        sql = (
            "SELECT o.position, e.event_id, e.stream, e.seq, e.type, "
            "e.account_id, e.amount FROM outbox o "
            "JOIN events e ON e.event_id = o.event_id "
            "WHERE o.position > %s ORDER BY o.position ASC"
        )
        params: tuple = (after_id,)
        if limit is not None:
            sql += " LIMIT %s"
            params = (after_id, limit)
        with self._pool.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        events = []
        for row in rows:
            event = self._row_to_event(row)
            event["id"] = int(row["position"])
            events.append(event)
        return events

    def head_id(self) -> int:
        """Highest consumable position (relays first). 0 when empty."""
        self.relay_outbox()
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(position), 0) AS head FROM outbox"
            ).fetchone()
        return int(row["head"]) if row else 0

    @staticmethod
    def _row_to_event(row: dict) -> dict:
        event = {
            "event_id": row["event_id"],
            "stream": row["stream"],
            "seq": int(row["seq"]),
            "type": row["type"],
        }
        if row["account_id"] is not None:
            event["account_id"] = row["account_id"]
        if row["amount"] is not None:
            event["amount"] = int(row["amount"])
        return event


__all__ = ["PostgresEventStore"]
