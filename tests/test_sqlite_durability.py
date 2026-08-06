"""SqliteStore's reason to exist: the guard survives the process.

The shared chaos bench in conftest already attacks SqliteStore with the same
storms as the other backends. What it cannot test is durability, because
MemoryStore has none -- so these tests live here.

The claim under test: close the store (or crash the process) and the record
of "this already ran" is still there when you come back. That is the whole
difference between MemoryStore and SqliteStore, and the reason a single-box
deployment can stop double-charging without running a database server.
"""
from __future__ import annotations

import threading

from once import Once
from once.sqlite import SqliteStore


class FakeClock:
    def __init__(self, start: float = 1_000_000.0) -> None:
        self._now = start
        self._mu = threading.Lock()

    def __call__(self) -> float:
        with self._mu:
            return self._now

    def advance(self, sec: float) -> None:
        with self._mu:
            self._now += sec


def test_completed_effect_survives_reopen(tmp_path) -> None:
    """A second process opening the same file must NOT re-run the effect."""
    db = str(tmp_path / "once.db")
    clock = FakeClock()
    calls = []

    # "Process 1" runs the effect.
    store1 = SqliteStore(db, clock=clock)
    once1 = Once(store1, clock=clock)
    first = once1.run("charge:order-1", {"amount": 4900}, lambda: calls.append(1) or "receipt-1")
    store1.close()

    assert first == "receipt-1"
    assert len(calls) == 1

    # "Process 2" -- brand new store object over the same file, as a restart
    # or a sibling worker would be.
    store2 = SqliteStore(db, clock=clock)
    once2 = Once(store2, clock=clock)
    second = once2.run("charge:order-1", {"amount": 4900}, lambda: calls.append(1) or "receipt-2")

    assert len(calls) == 1, "the effect ran twice across a restart"
    assert second == "receipt-1", "the replay must return the original result"
    store2.close()


def test_in_flight_lease_is_reclaimable_after_a_crash(tmp_path) -> None:
    """A worker that dies mid-effect must not wedge the key forever.

    The lease it held is dead once the clock passes it, so a later worker
    takes over with a bumped generation -- exactly what a crash-and-restart
    looks like from the outside.
    """
    db = str(tmp_path / "once.db")
    clock = FakeClock()

    # Worker 1 claims the key and then "crashes": never completes, never fails.
    store1 = SqliteStore(db, clock=clock)
    once1 = Once(store1, clock=clock, lease_sec=30.0)
    out = once1.begin("send:invite-7", {"to": "a@example.com"})
    assert out.execute
    gen_before = store1.get("send:invite-7").generation
    store1.close()

    # Time passes; the lease dies.
    clock.advance(31.0)

    # Worker 2 restarts against the same file and takes the work over.
    store2 = SqliteStore(db, clock=clock)
    once2 = Once(store2, clock=clock, lease_sec=30.0)
    retry = once2.begin("send:invite-7", {"to": "a@example.com"})

    assert retry.execute, "a dead lease must be reclaimable after a crash"
    rec = store2.get("send:invite-7")
    assert rec.generation > gen_before, "takeover must bump generation for fencing"
    store2.close()


def test_result_roundtrips_through_json(tmp_path) -> None:
    """Results are stored as JSON text; nested structures must come back intact."""
    db = str(tmp_path / "once.db")
    clock = FakeClock()
    store = SqliteStore(db, clock=clock)
    once = Once(store, clock=clock)

    payload = {"order": "o-9"}
    result = {"id": "ch_1", "cents": 4900, "meta": {"tags": ["a", "b"], "ok": True, "n": None}}
    once.run("charge:o-9", payload, lambda: result)
    store.close()

    reopened = SqliteStore(db, clock=clock)
    replayed = Once(reopened, clock=clock).run("charge:o-9", payload, lambda: {"never": "run"})
    assert replayed == result
    reopened.close()
