# once in 5 minutes

You have an action that must not happen twice — a charge, an email, a
provisioning call. Here is the whole integration.

## 1. Install (30 seconds)

```bash
pip install "once-kernel[postgres]"   # or just once-kernel for single-process
```

## 2. Wrap the action (2 minutes)

```python
from once import Once

o = Once()  # in-memory; see step 4 for Postgres

def charge():
    return gateway.charge(order_id="ord_1", amount_cents=4900)

result = o.run(
    "pay:ord_1",                     # stable key = one logical action
    {"order": "ord_1", "amount_cents": 4900},   # payload fingerprint
    charge,
)
```

That's it. Retries, double-clicks, duplicate webhooks, agent re-fires —
`charge()` executes once; every other caller gets `result` replayed.

## 3. What each guarantee means (1 minute)

| If this happens | You get |
|---|---|
| Same key + same payload again | The stored result. **No second execution.** |
| Same key + **different** payload | `IdempotencyConflict` — never someone else's result |
| Two callers at the same instant | One executes; the rest coalesce onto its result |
| The executing worker dies | After the lease expires, another caller takes over |
| A "dead" worker wakes up late | Fenced out — it cannot corrupt the record |
| Your action raises | Key is freed; a later retry may execute again |

Honest model: **exactly-once execution, at-least-once result delivery.**
(True network exactly-once does not exist; anyone promising it is wrong.)

## 4. Production: share the memory (90 seconds)

One process is protected out of the box. Multiple workers/boxes share
state through your existing Postgres:

```python
from once import Once
from once.pg import PostgresStore

store = PostgresStore("postgresql://user:pass@host/db")  # table auto-created
o = Once(store)
```

Every operation the store performs is a single conditional SQL statement —
the database's own atomicity is the referee. No advisory locks, no
transactions spanning your code.

## 5. Long jobs: heartbeat

If an action can outlive the lease (default 30s), extend it while working:

```python
out = o.begin("job:big", {"x": 1})
if out.execute:
    for chunk in work_chunks:
        process(chunk)
        o.heartbeat("job:big", out.record.fence_token)
    o.complete("job:big", out.record.fence_token, {"done": True})
```

## 6. Async (FastAPI, agents)

```python
from once import AsyncOnce

ao = AsyncOnce()  # or AsyncOnce(Once(PostgresStore(...)))

@app.post("/charge")
async def charge_endpoint(req: ChargeReq):
    return await ao.run(f"pay:{req.order_id}", req.model_dump(), lambda: do_charge(req))
```

Sync side effects run in a worker thread — the event loop never stalls.
Waiters park on the event loop — no thread-pool starvation under
duplicate storms. (Both claims are enforced by tests in `tests/`.)

## Choosing keys (the one thing you must get right)

- Key = the *identity of the action*: `pay:{order_id}`, `email:{message_id}`.
- Same action retried → same key. New action → new key.
- Scope keys by tenant if ids can collide: `t42:pay:{order_id}`.
- The payload is your safety net: if a different body reuses a key,
  `once` refuses loudly instead of replaying the wrong result.

## See it survive an attack

```bash
python -m once.demo    # 1,000 concurrent duplicate charges -> 1 execution
```
