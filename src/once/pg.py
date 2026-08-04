"""
once.pg — PostgresStore: the production Store.

Design rules (each one is a correctness decision, not style):

1. **Every mutation is ONE SQL statement.** The database's own atomicity is
   the mutex — no read-modify-write in Python, no advisory locks, no
   transactions spanning app logic. If the statement's WHERE clause doesn't
   hold, rowcount is 0 and the caller lost. That IS the CAS.

2. **Time comes from the injected clock, never from `NOW()`.** The kernel,
   the store, and the tests share one clock, so lease/TTL behavior is
   deterministic under test (FakeClock) and consistent in production
   (time.time). SQL receives `now` as a parameter.

3. **`generation` increments on every takeover** — fresh claim over an
   expired record, and every dead-lease reclaim. Downstream systems can
   reject stale writers with `WHERE $gen >= their_stored_gen` (classic
   fencing for storage we don't control).

Install: pip install "once-kernel[postgres]"
Schema: created automatically (CREATE TABLE IF NOT EXISTS) on first use, or
run `PostgresStore.schema_sql("once_records")` yourself in a migration.
"""
from __future__ import annotations

import re
import time
from typing import Any, Callable, Optional

try:
    from psycopg.types.json import Jsonb
    from psycopg_pool import ConnectionPool
except ImportError as e:  # pragma: no cover
    raise ImportError(
        "PostgresStore requires the postgres extra: "
        'pip install "once-kernel[postgres]"'
    ) from e

from .kernel import Record, Status

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


class PostgresStore:
    """
    Multi-process, multi-box Store on PostgreSQL. Linearizable per key via
    single-statement conditional writes on the primary-key row.
    """

    def __init__(
        self,
        conninfo: str,
        *,
        table: str = "once_records",
        clock: Optional[Callable[[], float]] = None,
        min_size: int = 1,
        max_size: int = 10,
        create_schema: bool = True,
    ) -> None:
        if not _IDENT.match(table):
            raise ValueError(f"invalid table identifier: {table!r}")
        self._t = table
        self._clock: Callable[[], float] = clock or time.time
        self._pool = ConnectionPool(
            conninfo,
            min_size=min_size,
            max_size=max_size,
            kwargs={"autocommit": True},
            open=True,
        )
        if create_schema:
            with self._pool.connection() as conn:
                conn.execute(self.schema_sql(table))

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
            generation       BIGINT NOT NULL DEFAULT 1,
            result           JSONB,
            error            TEXT,
            created_at       DOUBLE PRECISION NOT NULL,
            updated_at       DOUBLE PRECISION NOT NULL,
            expires_at       DOUBLE PRECISION,
            lease_expires_at DOUBLE PRECISION
        )
        """

    # ------------------------------------------------------------------ #
    # Store protocol
    # ------------------------------------------------------------------ #

    def get(self, key: str) -> Optional[Record]:
        now = self._clock()
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""SELECT key, payload_hash, status, fence_token, generation,
                           result, error, created_at, updated_at,
                           expires_at, lease_expires_at
                    FROM {self._t} WHERE key = %s""",
                (key,),
            ).fetchone()
            if row is None:
                return None
            rec = self._row_to_record(row)
            if rec.is_expired(now):
                # Conditional delete: only if it is STILL the same expired row.
                conn.execute(
                    f"""DELETE FROM {self._t}
                        WHERE key = %s AND expires_at IS NOT NULL
                          AND expires_at <= %s""",
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
        with self._pool.connection() as conn:
            cur = conn.execute(
                f"""INSERT INTO {self._t} AS o
                        (key, payload_hash, status, fence_token, generation,
                         result, error, created_at, updated_at,
                         expires_at, lease_expires_at)
                    VALUES (%(key)s, %(ph)s, 'in_progress', %(fence)s, 1,
                            NULL, NULL, %(now)s, %(now)s,
                            %(exp)s, %(lease_exp)s)
                    ON CONFLICT (key) DO UPDATE SET
                        payload_hash     = EXCLUDED.payload_hash,
                        status           = 'in_progress',
                        fence_token      = EXCLUDED.fence_token,
                        generation       = o.generation + 1,
                        result           = NULL,
                        error            = NULL,
                        created_at       = EXCLUDED.created_at,
                        updated_at       = EXCLUDED.updated_at,
                        expires_at       = EXCLUDED.expires_at,
                        lease_expires_at = EXCLUDED.lease_expires_at
                    WHERE o.expires_at IS NOT NULL AND o.expires_at <= %(now)s""",
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
        with self._pool.connection() as conn:
            cur = conn.execute(
                f"""UPDATE {self._t} SET
                        status           = 'completed',
                        result           = %(result)s,
                        updated_at       = %(now)s,
                        lease_expires_at = NULL,
                        expires_at       = CASE
                            WHEN %(ttl)s::float8 IS NULL THEN expires_at
                            ELSE %(now)s + %(ttl)s
                        END
                    WHERE key = %(key)s
                      AND status = 'in_progress'
                      AND fence_token = %(fence)s
                      AND (expires_at IS NULL OR expires_at > %(now)s)""",
                {
                    "key": key,
                    "fence": fence_token,
                    "result": Jsonb(result),
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
        with self._pool.connection() as conn:
            if allow_retry:
                cur = conn.execute(
                    f"""DELETE FROM {self._t}
                        WHERE key = %(key)s
                          AND status = 'in_progress'
                          AND fence_token = %(fence)s
                          AND (expires_at IS NULL OR expires_at > %(now)s)""",
                    {"key": key, "fence": fence_token, "now": now},
                )
                return cur.rowcount == 1
            cur = conn.execute(
                f"""UPDATE {self._t} SET
                        status           = 'failed',
                        error            = %(error)s,
                        updated_at       = %(now)s,
                        lease_expires_at = NULL,
                        expires_at       = CASE
                            WHEN %(ttl)s::float8 IS NULL THEN expires_at
                            ELSE %(now)s + %(ttl)s
                        END
                    WHERE key = %(key)s
                      AND status = 'in_progress'
                      AND fence_token = %(fence)s
                      AND (expires_at IS NULL OR expires_at > %(now)s)""",
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
        with self._pool.connection() as conn:
            cur = conn.execute(
                f"""UPDATE {self._t} SET
                        fence_token      = %(fence)s,
                        generation       = generation + 1,
                        updated_at       = %(now)s,
                        lease_expires_at = %(now)s + %(lease)s,
                        expires_at       = CASE
                            WHEN %(ttl)s::float8 IS NULL THEN expires_at
                            ELSE %(now)s + %(ttl)s
                        END
                    WHERE key = %(key)s
                      AND status = 'in_progress'
                      AND payload_hash = %(ph)s
                      AND lease_expires_at IS NOT NULL
                      AND lease_expires_at <= %(now)s
                      AND (expires_at IS NULL OR expires_at > %(now)s)""",
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
        with self._pool.connection() as conn:
            cur = conn.execute(
                f"""UPDATE {self._t} SET
                        lease_expires_at = %(lease_exp)s,
                        updated_at       = %(now)s
                    WHERE key = %(key)s
                      AND status = 'in_progress'
                      AND fence_token = %(fence)s
                      AND (expires_at IS NULL OR expires_at > %(now)s)""",
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
        self._pool.close()

    def drop_table(self) -> None:
        """Test-bench helper. Not for production code paths."""
        with self._pool.connection() as conn:
            conn.execute(f"DROP TABLE IF EXISTS {self._t}")

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
            result=result,
            error=error,
            created_at=created_at,
            updated_at=updated_at,
            expires_at=expires_at,
            lease_expires_at=lease_expires_at,
            generation=generation,
        )
