"""Correctness tests — the cases that make enterprise care."""
from __future__ import annotations

import threading
import time

import pytest

from once import IdempotencyConflict, InProgressError, Once, Status
from once.kernel import MemoryStore, payload_hash


def test_payload_hash_stable():
    assert payload_hash({"b": 1, "a": 2}) == payload_hash({"a": 2, "b": 1})
    assert payload_hash({"a": 1}) != payload_hash({"a": 2})


def test_same_key_same_payload_replays():
    o = Once(MemoryStore())
    calls = {"n": 0}

    def charge():
        calls["n"] += 1
        return {"charged": 49}

    r1 = o.run("pay:order-1", {"amount": 49, "currency": "USD"}, charge)
    r2 = o.run("pay:order-1", {"amount": 49, "currency": "USD"}, charge)
    assert r1 == r2 == {"charged": 49}
    assert calls["n"] == 1  # side effect once


def test_same_key_different_payload_conflicts():
    o = Once(MemoryStore())
    o.run("pay:order-2", {"amount": 49}, lambda: "ok")
    with pytest.raises(IdempotencyConflict):
        o.run("pay:order-2", {"amount": 99}, lambda: "nope")


def test_fence_blocks_stale_worker_complete():
    """
    The classic corruption path:
    worker A begins, lease dies, worker B reclaims and completes,
    slow A must not complete with a second side-effect result.
    """
    store = MemoryStore()
    o = Once(store, lease_sec=0.05)

    out_a = o.begin("job:1", {"x": 1}, lease_sec=0.05)
    assert out_a.execute
    token_a = out_a.record.fence_token

    time.sleep(0.08)  # lease dead

    out_b = o.begin("job:1", {"x": 1}, lease_sec=30)
    assert out_b.execute
    token_b = out_b.record.fence_token
    assert token_a != token_b

    o.complete("job:1", token_b, {"winner": "B"})

    with pytest.raises(RuntimeError, match="fenced out"):
        o.complete("job:1", token_a, {"winner": "A"})

    rec = store.get("job:1")
    assert rec is not None
    assert rec.result == {"winner": "B"}


def test_concurrent_begin_single_executor():
    store = MemoryStore()
    o = Once(store)
    winners = []
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        try:
            out = o.begin("fanout:1", {"sku": "A"})
            if out.execute:
                winners.append(out.record.fence_token)
                time.sleep(0.05)
                o.complete("fanout:1", out.record.fence_token, {"ok": True})
        except InProgressError:
            pass

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(winners) == 1
    rec = store.get("fanout:1")
    assert rec is not None and rec.status == Status.COMPLETED
