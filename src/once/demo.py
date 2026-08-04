"""
The Storm Demo — `python -m once.demo`

1,000 concurrent attempts to charge the same order. Watch the kernel elect ONE.

This is the moment the library exists for: a retry storm hits a payment
endpoint at machine speed, and exactly one charge reaches the processor —
every other caller receives the same verified answer. No locks in your app
code, no bespoke idempotency table, no 3 AM.
"""
from __future__ import annotations

import threading
import time

from once import InProgressError, Once


def run_storm(attempts: int = 1000, workers: int = 50) -> dict:
    o = Once()
    executed: list[float] = []
    replayed = [0]
    waited = [0]
    mu = threading.Lock()

    AMOUNT = 4_900  # cents — the charge the storm is trying to duplicate

    def the_actual_charge() -> dict:
        with mu:
            executed.append(time.time())
        time.sleep(0.02)  # a realistic payment-gateway round trip
        return {"charge_id": "ch_storm_1", "amount_cents": AMOUNT}

    def attacker() -> None:
        try:
            r = o.run(
                "pay:order-777",
                {"order": 777, "amount_cents": AMOUNT},
                the_actual_charge,
                retry_sleep_sec=0.01,
            )
            with mu:
                if r == {"charge_id": "ch_storm_1", "amount_cents": AMOUNT}:
                    replayed[0] += 1
        except InProgressError:
            with mu:
                waited[0] += 1

    start = time.time()
    threads: list[threading.Thread] = []
    fired = 0
    while fired < attempts:
        batch = min(workers, attempts - fired)
        wave = [threading.Thread(target=attacker) for _ in range(batch)]
        for t in wave:
            t.start()
        for t in wave:
            t.join()
        threads.extend(wave)
        fired += batch
    elapsed = time.time() - start

    return {
        "attempts": attempts,
        "executions": len(executed),
        "callers_served_same_answer": replayed[0],
        "told_to_wait": waited[0],
        "dollars_defended": (attempts - len(executed)) * AMOUNT / 100,
        "elapsed_sec": round(elapsed, 2),
    }


def main() -> None:
    print()
    print("  ⚡ once — STORM DEMO")
    print("  1,000 concurrent attempts to charge order #777 ($49.00)…")
    print()
    s = run_storm()
    print(f"  attempts            : {s['attempts']:>6}")
    print(f"  ACTUAL EXECUTIONS   : {s['executions']:>6}   ← the whole point")
    print(f"  served stored answer: {s['callers_served_same_answer']:>6}")
    print(f"  told to wait        : {s['told_to_wait']:>6}")
    print(f"  elapsed             : {s['elapsed_sec']:>6}s")
    print()
    print(f"  💰 double-spend prevented this run: ${s['dollars_defended']:,.2f}")
    print()
    if s["executions"] == 1:
        print("  ✅ ONE execution. Every other caller got the same verified answer.")
    else:
        print("  ❌ KERNEL FAILURE — file a bug with this output immediately.")
    print()


if __name__ == "__main__":
    main()
