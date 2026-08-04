# once

**Side effects under retries run once. Same key + different body is an error — never a silent reuse.**

Networks retry. Users double-click. Workers redeliver. AI agents call tools twice.  
Most teams invent a half-broken idempotency table and corrupt money in production.

`once` is a tiny enterprise **idempotency kernel**: payload fingerprinting, leader election for the first request, **fence tokens** so a slow worker cannot complete after reclaim, and hard conflicts on payload mismatch.

```bash
pip install -e ".[dev]"   # from repo
pytest
```

```python
from once import Once

o = Once()  # MemoryStore; swap in Postgres/Redis Store for multi-process

def charge():
    return gateway.charge(order_id="ord_1", amount=49)

# Retries, double submits, agent fan-out → charge() runs once
result = o.run("pay:ord_1", {"order_id": "ord_1", "amount": 49}, charge)
```

## Who this is for

API, payments, webhooks, job consumers, and **Claude/agent tool writes** — anyone whose side effect must not double-apply.

## Not this

- Not a payment provider  
- Not an agent framework  
- Not “event sourcing for chat memory”

## Correctness guarantees (v0.1)

| Case | Behavior |
|------|----------|
| Same key + same payload, after success | Replay stored result — **no second side effect** |
| Same key + different payload | **`IdempotencyConflict`** |
| Concurrent first requests | **One** executor (CAS insert) |
| Lease expired mid-flight | Another worker may reclaim; **stale fence cannot complete** |
| In progress, live lease | **`InProgressError`** (wait / retry) |

## Guarantees (honest)

| Term | Meaning |
|------|---------|
| **Exactly-once execution** | Side effect body runs once per key+payload (under correct store) |
| **At-least-once delivery** | Clients may retry; they get a **replay**, not a second charge/email |

## Pre-launch store

**Postgres only.** Redis is v0.3+ (failover can drop acknowledged rows — wrong home for money truth).

## Docs

- `LOCKED.md` — architecture decisions
- `docs/STATE_MACHINE.md` — legal / illegal transitions
- `docs/PII_AND_RESULT_POLICY.md` — what we store, size limits

## Status

Alpha: MemoryStore + chaos CI + RFC 8785 + generation + heartbeat + wait coalesce + async façade + Postgres adapter (needs live DB). Dogfood helper for `nova_mailer`. Claude MCP next.

Apache-2.0
