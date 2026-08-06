"""
Shared test bench.

`once_factory` is parametrized over storage backends, so every test that uses
it runs against MemoryStore, a real on-disk SQLite file, and real PostgreSQL.
The chaos suite attacking every store identically is the point: the Store
protocol's guarantees are what's under test, not one implementation's luck.

Postgres bench: set ONCE_TEST_PG_DSN (default: postgresql://localhost/once_test).
If the database is unreachable, PG params skip with an explicit reason — they
never silently pass.
"""
from __future__ import annotations

import os
import threading
import uuid

import pytest

from once import Once
from once.kernel import MemoryStore

PG_DSN = os.environ.get("ONCE_TEST_PG_DSN", "postgresql://localhost/once_test")


class FakeClock:
    """Deterministic, thread-safe clock shared by kernel + store in tests."""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self._now = start
        self._mu = threading.Lock()

    def __call__(self) -> float:
        with self._mu:
            return self._now

    def advance(self, sec: float) -> None:
        with self._mu:
            self._now += sec


def _pg_available() -> bool:
    try:
        import psycopg

        with psycopg.connect(PG_DSN, connect_timeout=2):
            return True
    except Exception:
        return False


_PG_OK = _pg_available()


@pytest.fixture(params=["memory", "sqlite", "postgres"])
def once_factory(request, tmp_path_factory):
    """Returns factory(**once_kwargs) -> (Once, store, clock)."""
    backend = request.param
    if backend == "postgres" and not _PG_OK:
        if os.environ.get("ONCE_TEST_REQUIRE_PG") == "1":
            pytest.fail(
                f"postgres bench REQUIRED but unreachable at {PG_DSN} -- "
                "in CI a silent skip would report green without testing the "
                "production store; failing loudly instead"
            )
        pytest.skip(f"postgres bench unreachable at {PG_DSN}")

    created = []

    def factory(**kw):
        clock = FakeClock()
        if backend == "memory":
            store = MemoryStore(clock=clock)
        elif backend == "sqlite":
            from once.sqlite import SqliteStore

            # A real file, not ":memory:" -- each connection to ":memory:"
            # is a separate database, so the threads in the chaos suite
            # would each get their own and prove nothing.
            db = tmp_path_factory.mktemp("once") / f"{uuid.uuid4().hex[:12]}.db"
            store = SqliteStore(str(db), clock=clock)
            created.append(store)
        else:
            from once.pg import PostgresStore

            # Fresh table per test: isolation without cross-test bleed.
            store = PostgresStore(
                PG_DSN, table=f"once_t_{uuid.uuid4().hex[:12]}", clock=clock
            )
            created.append(store)
        return Once(store, clock=clock, **kw), store, clock

    yield factory

    for store in created:
        try:
            store.drop_table()
        finally:
            store.close()
