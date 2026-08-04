# State machine

Honest model: **exactly-once execution** of the side effect, **at-least-once delivery** of the HTTP/agent request (clients may retry and receive a replay).

## States

```
ABSENT ──begin win──► IN_PROGRESS ──complete──► COMPLETED
                         │  │
                         │  └──fail(allow_retry)──► ABSENT
                         │
                         └──fail(terminal)──► FAILED
                         │
                         └──lease dead + reclaim──► IN_PROGRESS (generation+1, new fence)

COMPLETED ──begin same payload──► COMPLETED (replay)
FAILED    ──begin same payload──► FAILED (replay)
*         ──begin different payload──► CONFLICT (no transition)
```

## Illegal transitions (must reject)

| From | Action | Illegal if |
|------|--------|------------|
| IN_PROGRESS | complete | fence_token ≠ current |
| IN_PROGRESS | complete | already COMPLETED by other fence |
| IN_PROGRESS | reclaim | lease still live |
| IN_PROGRESS | reclaim | payload_hash mismatch |
| COMPLETED | complete | different fence (stale zombie) |
| ABSENT | complete | no row |
| any | begin | payload_hash ≠ stored (Conflict) |

## Generation

- Fresh claim: `generation = 1`
- Every successful reclaim: `generation += 1`
- Downstream stores: `WHERE generation >= $worker_generation` to reject stale writers
