"""Durable local state. SQLite in WAL mode on the local state volume.

Phase 0 persists exactly what restart recovery needs (DESIGN.md 11.3):
the enrolled node identity, the last accepted desired generation, and the
last heartbeat sequence within the current session. The operation journal
(11.1) arrives with engine lifecycle in Phase 2 and is intentionally absent
here.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional


class StateStore:
    def __init__(self, db_path: str | Path):
        p = Path(db_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(p), isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._migrate()

    def _migrate(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS identity (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                node_id TEXT NOT NULL,
                certificate TEXT NOT NULL,
                enrolled_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )

    # --- identity -------------------------------------------------------
    def save_identity(self, node_id: str, certificate: str, enrolled_at: str) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO identity (id, node_id, certificate, enrolled_at) VALUES (1, ?, ?, ?)",
            (node_id, certificate, enrolled_at),
        )

    def load_identity(self) -> Optional[tuple[str, str, str]]:
        row = self._conn.execute(
            "SELECT node_id, certificate, enrolled_at FROM identity WHERE id = 1"
        ).fetchone()
        return row if row is None else (row[0], row[1], row[2])

    # --- desired generation --------------------------------------------
    def accepted_generation(self) -> int:
        row = self._conn.execute("SELECT value FROM meta WHERE key = 'accepted_generation'").fetchone()
        return int(row[0]) if row else 0

    def set_accepted_generation(self, generation: int) -> None:
        """Monotonic floor. A lower generation is refused (DESIGN.md 17.5)."""
        current = self.accepted_generation()
        if generation < current:
            raise ValueError(f"generation regression: {generation} < accepted {current}")
        self._conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('accepted_generation', ?)",
            (str(generation),),
        )

    # --- heartbeat sequence --------------------------------------------
    def last_sequence(self) -> int:
        row = self._conn.execute("SELECT value FROM meta WHERE key = 'last_sequence'").fetchone()
        return int(row[0]) if row else 0

    def set_last_sequence(self, sequence: int) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_sequence', ?)",
            (str(sequence),),
        )

    def close(self) -> None:
        self._conn.close()
