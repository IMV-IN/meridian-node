"""Operation journal (DESIGN.md 11.1).

Every side effect that cannot be safely repeated is journaled before it runs and
committed after it is observed. The journal makes launch/stop idempotent across
a crash: a duplicate operation_id with the same request digest returns the prior
result; a reused id with a different digest is rejected; an `uncertain`
operation must be resolved by inspection, never blindly repeated.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

PLANNED = "planned"
STARTED = "started"
OBSERVED = "side_effect_observed"
COMMITTED = "committed"
UNCERTAIN = "uncertain"


class JournalConflict(Exception):
    pass


@dataclass
class JournalRow:
    operation_id: str
    request_digest: str
    engine_id: str
    side_effect_type: str
    state: str
    result: Optional[str]


class Journal:
    def __init__(self, db_path: str | Path):
        p = Path(db_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(p), isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS journal (
                operation_id TEXT PRIMARY KEY,
                request_digest TEXT NOT NULL,
                engine_id TEXT NOT NULL,
                fencing_epoch INTEGER NOT NULL,
                side_effect_type TEXT NOT NULL,
                state TEXT NOT NULL,
                result TEXT
            )
            """
        )

    def plan(
        self, operation_id: str, request_digest: str, engine_id: str, fencing_epoch: int, side_effect_type: str
    ) -> str:
        """Record intent. Returns the existing state for a duplicate operation_id.

        Rejects an id reused with a different request digest (DESIGN.md 11.1).
        """
        row = self._get(operation_id)
        if row is not None:
            if row.request_digest != request_digest:
                raise JournalConflict(f"operation {operation_id} reused with a different request digest")
            return row.state
        self._conn.execute(
            "INSERT INTO journal (operation_id, request_digest, engine_id, fencing_epoch, side_effect_type, state) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (operation_id, request_digest, engine_id, fencing_epoch, side_effect_type, PLANNED),
        )
        return PLANNED

    def mark(self, operation_id: str, state: str, result: Optional[str] = None) -> None:
        self._conn.execute(
            "UPDATE journal SET state = ?, result = COALESCE(?, result) WHERE operation_id = ?",
            (state, result, operation_id),
        )

    def inspect(self, operation_id: str) -> Optional[JournalRow]:
        return self._get(operation_id)

    def pending_operations(self) -> list[JournalRow]:
        """Rows that crashed mid-flight: not committed and not yet observed."""
        cur = self._conn.execute(
            "SELECT operation_id, request_digest, engine_id, side_effect_type, state, result "
            "FROM journal WHERE state IN (?, ?, ?)",
            (PLANNED, STARTED, UNCERTAIN),
        )
        return [JournalRow(*r) for r in cur.fetchall()]

    def _get(self, operation_id: str) -> Optional[JournalRow]:
        r = self._conn.execute(
            "SELECT operation_id, request_digest, engine_id, side_effect_type, state, result "
            "FROM journal WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        return JournalRow(*r) if r else None

    def close(self) -> None:
        self._conn.close()
