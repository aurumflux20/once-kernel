"""
Async façade + dogfood-wrapper correctness.

Two of these tests exist because the audit found bugs their absence allowed:
- the event-loop-stall test (sync fn used to run ON the loop);
- the dogfood replay test (default kernel used to be a fresh, amnesiac
  Once() per call — idempotency theater).
Plain pytest + asyncio.run — no plugin dependency.
"""
from __future__ import annotations

import asyncio
import threading
import time

import pytest

from once import AsyncOnce, MemoryStore, Once
from once.dogfood_mailer import send_card_once


# ---------------------------------------------------------------------------
# AsyncOnce
# ---------------------------------------------------------------------------

def test_async_storm_one_execution_all_coalesce():
    """20 concurrent tasks race AsyncOnce.run on one key: fn fires once."""

    async def main():
        ao = AsyncOnce(Once(MemoryStore()))
        calls = {"n": 0}
        mu = threading.Lock()

        def effect():  # sync side effect -> must go to a worker thread
            with mu:
                calls["n"] += 1
            time.sleep(0.03)
            return {"charge_id": "ch_async"}

        results = await asyncio.gather(
            *(
                ao.run("pay:async", {"amt": 49}, effect, wait_timeout_sec=10.0)
                for _ in range(20)
            )
        )
        return calls["n"], results

    n, results = asyncio.run(main())
    assert n == 1, "side effect fired more than once under async storm"
    assert len(results) == 20
    assert all(r == {"charge_id": "ch_async"} for r in results)


def test_async_native_coroutine_fn_supported():
    async def main():
        ao = AsyncOnce(Once(MemoryStore()))
        calls = {"n": 0}

        async def effect():
            calls["n"] += 1
            await asyncio.sleep(0.01)
            return {"ok": True}

        r1 = await ao.run("job:coro", {"x": 1}, effect)
        r2 = await ao.run("job:coro", {"x": 1}, effect)
        return calls["n"], r1, r2

    n, r1, r2 = asyncio.run(main())
    assert n == 1 and r1 == r2 == {"ok": True}


def test_sync_fn_does_not_stall_the_event_loop():
    """
    While a SLOW sync side effect runs via AsyncOnce, the loop must keep
    serving other coroutines. Before the audit fix, fn() executed directly
    on the loop and this heartbeat count stayed at ~0.
    """

    async def main():
        ao = AsyncOnce(Once(MemoryStore()))
        beats = {"n": 0}
        stop = asyncio.Event()

        async def loop_heartbeat():
            while not stop.is_set():
                beats["n"] += 1
                await asyncio.sleep(0.01)

        def slow_sync_effect():
            time.sleep(0.3)  # a slow payment-gateway call
            return {"ok": True}

        hb = asyncio.create_task(loop_heartbeat())
        await ao.run("job:slow", {"x": 1}, slow_sync_effect)
        stop.set()
        await hb
        return beats["n"]

    beats = asyncio.run(main())
    assert beats >= 10, (
        f"event loop starved during sync side effect (only {beats} beats) -- "
        "sync fn must run in a worker thread"
    )


def test_async_run_replays_failure_as_error():
    async def main():
        ao = AsyncOnce(Once(MemoryStore()))

        def boom():
            raise ValueError("gateway down")

        with pytest.raises(ValueError):
            await ao.run("job:boom", {"x": 1}, boom)
        # allow_retry=True freed the key; a retry may execute again.
        out = await ao.begin("job:boom", {"x": 1})
        return out.execute

    assert asyncio.run(main()) is True


# ---------------------------------------------------------------------------
# Dogfood wrapper (nova_mailer pattern)
# ---------------------------------------------------------------------------

def test_send_card_once_second_approve_replays_not_resends():
    """The double-approve scenario this wrapper exists for."""
    o = Once(MemoryStore())
    sends = {"n": 0}

    def fake_send():
        sends["n"] += 1
        return {"ok": True, "sent": True, "message_id": "re_123", "to": "x@y.com"}

    decision = {"id": 4242, "meta": {"product": "bondpermit", "to": "x@y.com"}}

    r1 = send_card_once(decision, fake_send, once=o)
    r2 = send_card_once(decision, fake_send, once=o)  # the double-click

    assert sends["n"] == 1, "second approve must NOT trigger a second send"
    assert r1 == r2
    assert r1["sent"] is True


def test_send_card_once_result_contains_no_raw_email():
    """PII policy: the stored result may never contain the raw address."""
    o = Once(MemoryStore())

    def fake_send():
        return {"ok": True, "sent": True, "message_id": "re_9", "to": "sec@ret.com"}

    decision = {"id": 777, "meta": {"product": "glint", "to": "sec@ret.com"}}
    result = send_card_once(decision, fake_send, once=o)

    flat = repr(result)
    assert "sec@ret.com" not in flat, "raw email leaked into stored result"
    assert result.get("to_hash"), "hashed recipient expected for support triage"


def test_send_card_once_default_kernel_actually_remembers():
    """
    Regression for the amnesia bug: calling WITHOUT an explicit kernel must
    still dedupe within the process (module-level default, not a fresh Once).
    """
    sends = {"n": 0}

    def fake_send():
        sends["n"] += 1
        return {"ok": True, "sent": True, "message_id": "re_amn", "to": "a@b.co"}

    decision = {"id": "amnesia-check-1", "meta": {"product": "glint"}}
    send_card_once(decision, fake_send)
    send_card_once(decision, fake_send)

    assert sends["n"] == 1, (
        "default-kernel calls forgot each other -- idempotency theater is back"
    )
