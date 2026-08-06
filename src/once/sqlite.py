"""
once.sqlite — SqliteStore: durable, multi-process, zero dependencies.

The gap this fills: MemoryStore dies with the process and protects nothing
once you run a second worker; PostgresStore protects everything but wants a
server. A great many real deployments are one box with a few processes, and
for those SQLite is the honest answer.

Design rules, identical to PostgresStore because they are correctness
decisions rather than style:

1. **Every mutation is ONE SQL statement.** SQLite's own atomicity is the
   mutex -- no read-modify-write in Python. If the WHERE clause does not
   hold, rowcount is 0 and the caller lost. That IS the compare-and-swap.

2. **Time comes from the injected clock, never from SQL.** Kernel, store and
   tests share one clock, so lease and TTL behaviour is deterministic under
   FakeClock.

3. **`generation` increments on every takeover** -- a fresh claim over an
   expired record, and every dead-lease reclaim -- so downstream systems can
   fence stale writers.

Concurrency notes (the part that makes SQLite actually safe here):

- **WAL mode** so readers never block the writer and separate processes can
  work concurrently.
- **`busy_timeout`** so a contended write waits for the lock instead of
  raising `database is locked` -- which is what makes the storm survivable.
- **One connection per thread** (`threading.local`). `sqlite3` connections
  are not safe to share across threads, and the chaos suite runs real ones.
- **`isolation_level=None`** (autocommit), matching PostgresStore: each
  statement stands alone, no transaction spans application logic.

Install: nothing. `sqlite3` is in the standard library.
"""
from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Optional

from .kernel import Record, Status

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


class SqliteStore:
    """
    Durable Store on SQLite. Survives restarts, and is safe across threads
    and across processes on one machine.

    Not a network database: every process must be able to open the same file.
    For several boxes, use PostgresStore.
    """

    def __init__(
        self,
        path: str,
        *,
        table: str = "once_records",
        clock: Optional[Callable[[], float]] = None,
        create_schema: bool = True,
        busy_timeout_sec: float = 5.0,
    ) -> None:
        if not _IDENT.match(table):
            raise ValueError(f"invalid table identifier: {table!r}")
        self._t = table
        self._path = path
        self._clock: Callable[[], float] = clock or time.time
        self._busy_timeout_ms = int(busy_timeout_sec * 1000)
        self._local = threading.local()
        if create_schema:
            self._conn().executescript(self.schema_sql(table))

    # ------------------------------------------------------------------ #
    # Connection handling
    # ------------------------------------------------------------------ #

    def _conn(self) -> sqlite3.Connection:
        """One connection per thread. sqlite3 connections are not shareable."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self._path, isolation_level=None)
            # WAL lets readers and a writer coexist, across processes too.
            # Memory databases do not support WAL, and each connection to
            # ":memory:" is a separate database anyway -- so this store is
            # only meaningful against a real file.
            try:
                conn.execute("PRAGMA journal_mode=WAL")
            except sqlite3.DatabaseError:  # pragma: no cover
                pass
            conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
            # FULL would fsync on every commit; NORMAL is the usual WAL
            # pairing and still crash-safe for our purposes.
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return conn

    @staticmethod
    def schema_sql(table: str) -> str:
        if not _IDENT.match(table):
            raise ValueError(f"invalid table identifier: {table!r}")
        return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            key              TEXT PRIMARY KEY,
            payload_hash     TEXT NOT NULL,
            status           TEXT NOT NULL
                             CHECK (status IN ('in_progress','completed','failed')),
            fence_token      TEXT NOT NULL,
            generation       INTEGER NOT NULL DEFAULT 1,
            result           TEXT,
            error            TEXT,
            created_at       REAL NOT NULL,
            updated_at       REAL NOT NULL,
            expires_at       REAL,
            lease_expires_at REAL
        );
        """

    # ------------------------------------------------------------------ #
    # Store protocol
    # ------------------------------------------------------------------ #

    def get(self, key: str) -> Optional[Record]:
        now = self._clock()
        conn = self._conn()
        row = conn.execute(
            f"""SELECT key, payload_hash, status, fence_token, generation,
                       result, error, created_at, updated_at,
                       expires_at, lease_expires_at
                FROM {self._t} WHERE key = ?""",
            (key,),
        ).fetchone()
        if row is None:
            return None
        rec = self._row_to_record(row)
        if rec.is_expired(now):
            # Conditional delete: only if it is STILL the same expired row,
            # so we never clobber a fresh claim that raced us.
            conn.execute(
                f"""DELETE FROM {self._t}
                    WHERE key = ? AND expires_at IS NOT NULL
                      AND expires_at <= ?""",
                (key, now),
            )
            return None
        return rec

    def create_in_progress(
        self,
        key: str,
        payload_hash: str,
        fence_token: str,
        *,
        ttl_sec: Optional[float],
        lease_sec: float,
    ) -> bool:
        now = self._clock()
        exp = (now + ttl_sec) if ttl_sec is not None else None
        cur = self._conn().execute(
            f"""INSERT INTO {self._t}
                    (key, payload_hash, status, fence_token, generation,
                     result, error, created_at, updated_at,
                     expires_at, lease_expires_at)
                VALUES (:key, :ph, 'in_progress', :fence, 1,
                        NULL, NULL, :now, :now,
                        :exp, :lease_exp)
                ON CONFLICT(key) DO UPDATE SET
                    payload_hash     = excluded.payload_hash,
                    status           = 'in_progress',
                    fence_token      = excluded.fence_token,
                    generation       = generation + 1,
                    result           = NULL,
                    error            = NULL,
                    created_at       = excluded.created_at,
                    updated_at       = excluded.updated_at,
                    expires_at       = excluded.expires_at,
                    lease_expires_at = excluded.lease_expires_at
                WHERE expires_at IS NOT NULL AND expires_at <= :now""",
            {
                "key": key,
                "ph": payload_hash,
                "fence": fence_token,
                "now": now,
                "exp": exp,
                "lease_exp": now + lease_sec,
            },
        )
        return cur.rowcount == 1

    def cas_complete(
        self,
        key: str,
        fence_token: str,
        result: Any,
        *,
        ttl_sec: Optional[float],
    ) -> bool:
        now = self._clock()
        cur = self._conn().execute(
            f"""UPDATE {self._t} SET
                    status           = 'completed',
                    result           = :result,
                    updated_at       = :now,
                    lease_expires_at = NULL,
                    expires_at       = CASE
                        WHEN :ttl IS NULL THEN expires_at
                        ELSE :now + :ttl
                    END
                WHERE key = :key
                  AND status = 'in_progress'
                  AND fence_token = :fence
                  AND (expires_at IS NULL OR expires_at > :now)""",
            {
                "key": key,
                "fence": fence_token,
                "result": json.dumps(result),
                "now": now,
                "ttl": ttl_sec,
            },
        )
        return cur.rowcount == 1

    def cas_fail(
        self,
        key: str,
        fence_token: str,
        error: str,
        *,
        ttl_sec: Optional[float],
        allow_retry: bool,
    ) -> bool:
        now = self._clock()
        conn = self._conn()
        if allow_retry:
            # Free the key so the same payload can begin again.
            cur = conn.execute(
                f"""DELETE FROM {self._t}
                    WHERE key = :key
                      AND status = 'in_progress'
                      AND fence_token = :fence
                      AND (expires_at IS NULL OR expires_at > :now)""",
                {"key": key, "fence": fence_token, "now": now},
            )
            return cur.rowcount == 1
        cur = conn.execute(
            f"""UPDATE {self._t} SET
                    status           = 'failed',
                    error            = :error,
                    updated_at       = :now,
                    lease_expires_at = NULL,
                    expires_at       = CASE
                        WHEN :ttl IS NULL THEN expires_at
                        ELSE :now + :ttl
                    END
                WHERE key = :key
                  AND status = 'in_progress'
                  AND fence_token = :fence
                  AND (expires_at IS NULL OR expires_at > :now)""",
            {
                "key": key,
                "fence": fence_token,
                "error": error,
                "now": now,
                "ttl": ttl_sec,
            },
        )
        return cur.rowcount == 1

    def reclaim_if_lease_dead(
        self,
        key: str,
        payload_hash: str,
        new_fence_token: str,
        *,
        lease_sec: float,
        ttl_sec: Optional[float],
    ) -> bool:
        now = self._clock()
        cur = self._conn().execute(
            f"""UPDATE {self._t} SET
                    fence_token      = :fence,
                    generation       = generation + 1,
                    updated_at       = :now,
                    lease_expires_at = :now + :lease,
                    expires_at       = CASE
                        WHEN :ttl IS NULL THEN expires_at
                        ELSE :now + :ttl
                    END
                WHERE key = :key
                  AND status = 'in_progress'
                  AND payload_hash = :ph
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at <= :now
                  AND (expires_at IS NULL OR expires_at > :now)""",
            {
                "key": key,
                "ph": payload_hash,
                "fence": new_fence_token,
                "now": now,
                "lease": lease_sec,
                "ttl": ttl_sec,
            },
        )
        return cur.rowcount == 1

    def heartbeat(
        self,
        key: str,
        fence_token: str,
        *,
        lease_sec: float,
    ) -> bool:
        now = self._clock()
        cur = self._conn().execute(
            f"""UPDATE {self._t} SET
                    lease_expires_at = :lease_exp,
                    updated_at       = :now
                WHERE key = :key
                  AND status = 'in_progress'
                  AND fence_token = :fence
                  AND (expires_at IS NULL OR expires_at > :now)""",
            {
                "key": key,
                "fence": fence_token,
                "now": now,
                "lease_exp": now + lease_sec,
            },
        )
        return cur.rowcount == 1

    # ------------------------------------------------------------------ #
    # Lifecycle / helpers
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    def drop_table(self) -> None:
        """Test-bench helper. Not for production code paths."""
        self._conn().execute(f"DROP TABLE IF EXISTS {self._t}")

    @staticmethod
    def _row_to_record(row: tuple) -> Record:
        (
            key,
            payload_hash,
            status,
            fence_token,
            generation,
            result,
            error,
            created_at,
            updated_at,
            expires_at,
            lease_expires_at,
        ) = row
        return Record(
            key=key,
            payload_hash=payload_hash,
            status=Status(status),
            fence_token=fence_token,
            result=json.loads(result) if result is not None else None,
            error=error,
            created_at=created_at,
            updated_at=updated_at,
            expires_at=expires_at,
            lease_expires_at=lease_expires_at,
            generation=generation,
        )
