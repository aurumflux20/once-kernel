"""
Chaos & adversarial suite — the torture chamber.

The unit tests prove the happy paths. THESE tests attack the kernel the way
production does: thread storms, dead leases, zombie workers, clock control,
payload adversaries. A correctness library earns trust here or nowhere.

Conventions:
- FakeClock drives every lease/TTL scenario — zero real sleeps for time logic,
  so the suite is fast AND deterministic.
- Barrier-synchronized thread storms force the races instead of hoping for them
  (lesson learned building EffectFence: unforced races basically never collide).
"""
from __future__ import annotations

import threading
import time

import pytest

from once import IdempotencyConflict, InProgressError, Once, Status
from once.kernel import MemoryStore, payload_hash


# ---------------------------------------------------------------------------
# Thread storms
# ---------------------------------------------------------------------------

def test_forced_concurrent_begin_exactly_one_winner_per_trial(once_factory):
    """100 independent trials × 8 threads: never zero winners, never two."""
    o, _, _ = once_factory()
    TRIALS, N = 100, 8

    for trial in range(TRIALS):
        key = f"storm:{trial}"
        winners: list[str] = []
        blocked = []
        barrier = threading.Barrier(N)

        def worker():
            barrier.wait()
            try:
                out = o.begin(key, {"sku": "A"})
                if out.execute:
                    winners.append(out.record.fence_token)
                else:
                    blocked.append("replay")
            except InProgressError:
                blocked.append("in_progress")

        threads = [threading.Thread(target=worker) for _ in range(N)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(winners) == 1, f"trial {trial}: winners={len(winners)}"
        assert len(blocked) == N - 1


def test_run_thread_storm_side_effect_exactly_once_result_identical(once_factory):
    """16 threads race o.run() on ONE key: fn fires once, all callers converge."""
    o, _, _ = once_factory()
    calls = {"n": 0}
    call_mu = threading.Lock()

    def effect():
        with call_mu:
            calls["n"] += 1
        return {"charge_id": "ch_1"}

    results: list = []
    errors: list = []
    barrier = threading.Barrier(16)

    def worker():
        barrier.wait()
        try:
            results.append(
                o.run("pay:storm", {"amt": 49}, effect, retry_sleep_sec=0.01)
            )
        except Exception as e:  # noqa: BLE001 — collecting, not hiding
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert calls["n"] == 1, "side effect fired more than once under storm"
    assert not errors, f"unexpected errors: {errors!r}"
    assert all(r == {"charge_id": "ch_1"} for r in results)
    assert len(results) == 16


# ---------------------------------------------------------------------------
# Dead leases, zombies, fencing
# ---------------------------------------------------------------------------

def test_concurrent_reclaim_exactly_one_takeover(once_factory):
    """Owner dies (clock passes lease). 8 vultures race: exactly one reclaims."""
    o, _, clock = once_factory(lease_sec=30)

    out = o.begin("job:reclaim", {"x": 1})
    assert out.execute
    dead_token = out.record.fence_token

    clock.advance(31)  # owner is now legally dead

    winners, waiting = [], []
    barrier = threading.Barrier(8)

    def vulture():
        barrier.wait()
        try:
            got = o.begin("job:reclaim", {"x": 1})
            if got.execute:
                winners.append(got.record.fence_token)
        except InProgressError:
            waiting.append(1)

    threads = [threading.Thread(target=vulture) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(winners) == 1
    assert len(waiting) == 7
    assert winners[0] != dead_token


def test_zombie_owner_fenced_out_of_both_complete_and_fail(once_factory):
    """The corruption classic: reclaimed key must reject the old token entirely."""
    o, store, clock = once_factory(lease_sec=30)

    a = o.begin("job:zombie", {"x": 1})
    token_a = a.record.fence_token

    clock.advance(31)
    b = o.begin("job:zombie", {"x": 1})
    assert b.execute
    token_b = b.record.fence_token

    o.complete("job:zombie", token_b, {"winner": "B"})

    with pytest.raises(RuntimeError, match="fenced out"):
        o.complete("job:zombie", token_a, {"winner": "zombie"})
    with pytest.raises(RuntimeError, match="fenced out"):
        o.fail("job:zombie", token_a, "zombie says it failed")

    rec = store.get("job:zombie")
    assert rec is not None and rec.result == {"winner": "B"}


def test_complete_same_token_retry_is_idempotent(once_factory):
    """Retrying complete() with the SAME token must not raise (network retry)."""
    o, _, _ = once_factory()
    out = o.begin("job:dup-complete", {"x": 1})
    o.complete("job:dup-complete", out.record.fence_token, {"ok": True})
    rec = o.complete("job:dup-complete", out.record.fence_token, {"ok": True})
    assert rec.status == Status.COMPLETED and rec.result == {"ok": True}


# ---------------------------------------------------------------------------
# Failure semantics
# ---------------------------------------------------------------------------

def test_terminal_failure_replays_error_and_still_conflicts_on_new_payload(once_factory):
    o, _, _ = once_factory()
    out = o.begin("job:term", {"x": 1})
    o.fail("job:term", out.record.fence_token, "card declined", allow_retry=False)

    replay = o.begin("job:term", {"x": 1})
    assert not replay.execute and replay.record.status == Status.FAILED
    assert replay.record.error == "card declined"

    # Terminal failure does NOT loosen the payload contract.
    with pytest.raises(IdempotencyConflict):
        o.begin("job:term", {"x": 2})


def test_allow_retry_failure_frees_the_key(once_factory):
    o, _, _ = once_factory()
    out = o.begin("job:retryable", {"x": 1})
    o.fail("job:retryable", out.record.fence_token, "timeout", allow_retry=True)

    again = o.begin("job:retryable", {"x": 1})
    assert again.execute, "retryable failure must free the key"


# ---------------------------------------------------------------------------
# Time: leases and TTL under a controlled clock (zero real sleeps)
# ---------------------------------------------------------------------------

def test_lease_death_and_ttl_expiry_under_fake_clock(once_factory):
    o, _, clock = once_factory(lease_sec=60, default_ttl_sec=3600)

    out = o.begin("job:time", {"x": 1})
    with pytest.raises(InProgressError):
        o.begin("job:time", {"x": 1})  # live lease blocks

    clock.advance(61)  # lease dead → reclaimable
    re = o.begin("job:time", {"x": 1})
    assert re.execute

    o.complete("job:time", re.record.fence_token, {"done": 1})
    assert not o.begin("job:time", {"x": 1}).execute  # replay inside TTL

    clock.advance(3601)  # replay horizon passed → key reopens
    fresh = o.begin("job:time", {"x": 1})
    assert fresh.execute, "expired record must reopen the key"
    # Silence the unused-variable lint noise honestly:
    assert out.record.key == "job:time"


# ---------------------------------------------------------------------------
# Payload adversaries
# ---------------------------------------------------------------------------

def test_payload_canonicalization_edges():
    # Key order never matters; list order always does; unicode is stable.
    assert payload_hash({"a": [1, 2], "b": "é"}) == payload_hash({"b": "é", "a": [1, 2]})
    assert payload_hash({"a": [1, 2]}) != payload_hash({"a": [2, 1]})
    # Nested dict ordering is canonicalized too.
    assert payload_hash({"o": {"x": 1, "y": 2}}) == payload_hash({"o": {"y": 2, "x": 1}})
    # NaN is rejected, not silently coerced (allow_nan=False).
    with pytest.raises(ValueError):
        payload_hash({"a": float("nan")})


def test_conflict_wins_over_replay_even_after_completion(once_factory):
    """A different body must NEVER receive the stored result of another body."""
    o, _, _ = once_factory()
    out = o.begin("pay:strict", {"amount": 49})
    o.complete("pay:strict", out.record.fence_token, {"charged": 49})

    with pytest.raises(IdempotencyConflict):
        o.begin("pay:strict", {"amount": 4900})


# ---------------------------------------------------------------------------
# Heartbeat + wait coalescing — parametrized over BOTH stores
# ---------------------------------------------------------------------------

def test_heartbeat_keeps_long_job_alive_past_original_lease(once_factory):
    """A slow-but-alive worker heartbeats; vultures must NOT reclaim it."""
    o, _, clock = once_factory(lease_sec=30)

    out = o.begin("job:long", {"x": 1})
    token = out.record.fence_token

    # 25s passes, worker heartbeats, 25 more pass (50 > original 30 lease).
    clock.advance(25)
    assert o.heartbeat("job:long", token), "live holder must be able to extend"
    clock.advance(25)

    with pytest.raises(InProgressError):
        o.begin("job:long", {"x": 1})  # still protected -- heartbeat worked

    o.complete("job:long", token, {"done": True})
    assert not o.begin("job:long", {"x": 1}).execute


def test_heartbeat_from_zombie_is_rejected_after_reclaim(once_factory):
    """After takeover, the old owner's heartbeat must fail -- fence holds."""
    o, _, clock = once_factory(lease_sec=30)

    a = o.begin("job:hb-zombie", {"x": 1})
    clock.advance(31)  # lease dead, no heartbeat came
    b = o.begin("job:hb-zombie", {"x": 1})
    assert b.execute

    assert not o.heartbeat("job:hb-zombie", a.record.fence_token), (
        "zombie heartbeat must be rejected after reclaim"
    )
    assert o.heartbeat("job:hb-zombie", b.record.fence_token)


def test_wait_true_coalesces_onto_winner_result(once_factory):
    """6 wait=True callers storm one key: exactly one executes, all coalesce."""
    o, _, _ = once_factory()
    barrier = threading.Barrier(6)
    executed, results, errors = [], [], []
    mu = threading.Lock()

    def caller():
        barrier.wait()
        try:
            out = o.begin("job:coalesce", {"x": 1}, wait=True, wait_timeout_sec=10.0)
            if out.execute:
                with mu:
                    executed.append(out.record.fence_token)
                time.sleep(0.05)  # the effect "runs"
                o.complete("job:coalesce", out.record.fence_token, {"n": 42})
                with mu:
                    results.append({"n": 42})
            else:
                with mu:
                    results.append(out.record.result)
        except Exception as e:  # noqa: BLE001
            with mu:
                errors.append(e)

    threads = [threading.Thread(target=caller) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"unexpected: {errors!r}"
    assert len(executed) == 1, "exactly one caller may win and execute"
    assert len(results) == 6
    assert all(r == {"n": 42} for r in results), (
        "every coalesced waiter must receive the winner's exact result"
    )


def test_wait_timeout_fires_in_real_time_even_with_frozen_clock(once_factory):
    """Caller patience is real time; a frozen simulation clock must not disable it."""
    from once.kernel import WaitTimeout

    o, _, _ = once_factory(lease_sec=3600)
    out = o.begin("job:stuck", {"x": 1})
    assert out.execute  # holder never completes; lease is huge; clock frozen

    with pytest.raises(WaitTimeout):
        o.begin("job:stuck", {"x": 1}, wait=True, wait_timeout_sec=0.3, poll_sec=0.02)
