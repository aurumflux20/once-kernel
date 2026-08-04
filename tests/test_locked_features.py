"""RFC 8785, generation, heartbeat, wait coalescing, result size, illegal transitions."""
from __future__ import annotations

import threading
import time

import pytest

from once import IdempotencyConflict, Once, Status
from once.canonical import canonical_bytes, payload_hash_hex
from once.kernel import (
    MemoryStore,
    ResultTooLarge,
    WaitTimeout,
    payload_hash,
)


def test_rfc8785_key_order_stable():
    a = payload_hash_hex({"b": 1, "a": 2})
    b = payload_hash_hex({"a": 2, "b": 1})
    assert a == b
    # JCS bytes, not hand-rolled separators-only
    assert canonical_bytes({"b": 1, "a": 2}) == b'{"a":2,"b":1}'


def test_generation_bumps_on_reclaim():
    clock = {"t": 1000.0}

    def clk():
        return clock["t"]

    store = MemoryStore(clock=clk)
    o = Once(store, clock=clk, lease_sec=1)
    out1 = o.begin("g:1", {"x": 1}, lease_sec=1)
    assert out1.record.generation == 1
    token1 = out1.record.fence_token
    clock["t"] += 2  # lease dead
    out2 = o.begin("g:1", {"x": 1}, lease_sec=10)
    assert out2.execute
    assert out2.record.generation == 2
    assert out2.record.fence_token != token1


def test_heartbeat_extends_lease():
    from once import InProgressError

    clock = {"t": 1000.0}

    def clk():
        return clock["t"]

    store = MemoryStore(clock=clk)
    o = Once(store, clock=clk, lease_sec=5)
    out = o.begin("h:1", {"x": 1}, lease_sec=5)
    assert o.heartbeat("h:1", out.record.fence_token, lease_sec=30)
    clock["t"] += 10  # original lease would be dead; heartbeat → live until 1030
    with pytest.raises(InProgressError):
        o.begin("h:1", {"x": 1}, wait=False)


def test_wait_coalesce_returns_same_result():
    o = Once(MemoryStore())
    results = []

    def worker():
        r = o.run("w:1", {"n": 1}, lambda: {"v": 42}, wait=True, wait_timeout_sec=5)
        results.append(r)

    # first holds lease briefly
    out = o.begin("w:1", {"n": 1}, wait=False)
    assert out.execute

    t = threading.Thread(target=worker)
    t.start()
    time.sleep(0.05)
    o.complete("w:1", out.record.fence_token, {"v": 42})
    t.join(timeout=3)
    assert results == [{"v": 42}]


def test_result_too_large():
    o = Once(MemoryStore())
    out = o.begin("big:1", {"a": 1})
    huge = {"blob": "x" * 70_000}
    with pytest.raises(ResultTooLarge):
        o.complete("big:1", out.record.fence_token, huge)


def test_illegal_stale_complete_after_reclaim():
    clock = {"t": 1.0}

    def clk():
        return clock["t"]

    store = MemoryStore(clock=clk)
    o = Once(store, clock=clk, lease_sec=1)
    a = o.begin("il:1", {"p": 1}, lease_sec=1)
    clock["t"] += 2
    b = o.begin("il:1", {"p": 1}, lease_sec=10)
    o.complete("il:1", b.record.fence_token, {"ok": True})
    with pytest.raises(RuntimeError, match="fenced out"):
        o.complete("il:1", a.record.fence_token, {"ok": False})
