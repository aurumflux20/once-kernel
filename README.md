# once

**Run any side effect exactly once — even when 1,000 callers demand it at the same instant.**

![1,000 concurrent duplicate charges, one execution](docs/storm.gif)

```
⚡ once — STORM DEMO
1,000 concurrent attempts to charge order #777 ($49.00)

ACTUAL EXECUTIONS   :      1   ← the whole point
served same answer  :  1,000 / 1,000
elapsed             :   0.1s

💰 double-spend prevented this run: $48,951.00
```

That's not a mock — it's a live attack you can run right now:

```bash
pip install once-kernel
python -m once.demo
```

## The problem

Networks retry. Users double-click. Queues redeliver. **AI agents re-fire tools at machine speed.** Any of these turns one payment into two, one email into three, one server into two hundred.

Most teams hand-roll an idempotency table — and most of those are [quietly broken under concurrent load](https://dev.to/chaitanya_srivastav_9bd5a/why-your-idempotency-implementation-is-probably-broken-under-concurrent-load-5b22): two identical requests both pass the "already done?" check, then both execute. The bugs are subtle, the failures are money.

`once` is that table done right, once, for everyone — a tiny **idempotency kernel** with the four defenses hand-rolled versions miss:

1. **Atomic leader election** — concurrent duplicates can't all pass the check; exactly one executes, the rest coalesce onto its result.
2. **Payload fingerprinting (RFC 8785)** — same key with a *different* body is a hard `IdempotencyConflict`, never someone else's cached answer.
3. **Fence tokens + generations** — a crashed worker's lease can be taken over, and when the "dead" worker wakes up late, it is *locked out* of corrupting the record.
4. **Honest failure states** — a failed attempt frees the key for retry; an unknown outcome never silently re-runs.

## Use it

```python
from once import Once

o = Once()

def charge():
    return gateway.charge(order_id="ord_1", amount_cents=4900)

# Retries, double submits, webhook redelivery, agent fan-out → runs ONCE
result = o.run("pay:ord_1", {"order": "ord_1", "amount_cents": 4900}, charge)
```

Multi-worker production — share state through the Postgres you already run:

```python
from once import Once
from once.pg import PostgresStore

o = Once(PostgresStore("postgresql://user:pass@host/db"))  # table auto-created
```

Async (FastAPI, agents) — sync side effects go to a worker thread, waiters park on the event loop (no thread-pool starvation under duplicate storms; there's a test that proves it):

```python
from once import AsyncOnce

ao = AsyncOnce()
result = await ao.run("pay:ord_1", payload, charge)
```

**[→ The full 5-minute guide](docs/FIVE_MINUTE_GUIDE.md)**

## What you can rely on

| If this happens | You get |
|---|---|
| Same key + same payload, again | The stored result — **no second execution** |
| Same key + **different** payload | `IdempotencyConflict` — never a silent wrong answer |
| 1,000 concurrent first requests | **One** executor; everyone else coalesces (`wait=True`) or is told to wait |
| Executing worker dies | Lease expires → another caller takes over |
| "Dead" worker wakes up late | **Fenced out** — cannot complete, cannot fail, cannot corrupt |
| Long job outliving its lease | `heartbeat()` keeps it protected |
| Your function raises | Key freed — a later retry may execute |

**The honest model** (put this on a poster): **exactly-once execution + at-least-once result delivery.** True network exactly-once is physically impossible — libraries claiming it are lying to you. We execute once and re-*deliver* the answer as many times as asked.

## Tested like money depends on it

Because it does. Every claim above is enforced by the chaos suite — barrier-forced thread storms, dead-lease reclaim stampedes, zombie-writer fencing, frozen-clock timeout attacks, event-loop-starvation detection — **run against both the in-memory store and real PostgreSQL on every commit** (CI fails loudly if the Postgres bench is skipped). Silence in CI never means "untested."

And we run it on our own production mailer — a double-approved send replays instead of double-emailing a real prospect. Dogfood first.

## Not this

- Not a payment provider — it guards *your* calls to one
- Not a workflow engine (no sagas, no multi-key transactions — [by decision](LOCKED.md))
- Not magic "exactly-once everywhere" — see the honest model above

## Docs

- [5-minute integration guide](docs/FIVE_MINUTE_GUIDE.md)
- [Full API reference](docs/API.md)
- [State machine — legal & illegal transitions](docs/STATE_MACHINE.md)
- [What we store: result size + PII policy](docs/PII_AND_RESULT_POLICY.md)
- [Architecture decisions](LOCKED.md)

Apache-2.0
