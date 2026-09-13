"""Readiness probe for the SQLite tier: a real read on each database file."""

from __future__ import annotations

import sqlite3

__all__ = ["SqliteReadinessProbe"]


class SqliteReadinessProbe:
    def __init__(self, *paths: str) -> None:
        if not paths:
            raise ValueError("at least one database path is required")
        self._paths = paths

    def check(self) -> dict[str, object]:
        checked: list[str] = []
        for path in self._paths:
            # Opening the file is not enough — a corrupt or locked database
            # only fails on a statement. Short timeout so a held lock cannot
            # hang the probe.
            conn = sqlite3.connect(path, timeout=1.0)
            try:
                conn.execute("SELECT 1").fetchone()
                conn.execute("PRAGMA schema_version").fetchone()
            finally:
                conn.close()
            checked.append(path)
        return {"storage": "ok", "databases": len(checked)}

    def close(self) -> None:
        return None
