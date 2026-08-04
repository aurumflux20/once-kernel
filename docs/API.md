# once — API reference

Complete surface. Everything here is enforced by the test suite against
both stores; anything not listed is private and may change.

---

## `Once` — the kernel

```python
Once(store=None, *, default_ttl_sec=86400, lease_sec=30.0, clock=None)
```

| Param | Meaning |
|---|---|
| `store` | A `Store` implementation. Default: fresh `MemoryStore`. Pass the SAME `clock` to a custom store. |
| `default_ttl_sec` | How long finished outcomes are remembered (replay horizon). `None` = forever. |
| `lease_sec` | How long an executing attempt is protected before it may be taken over. Must exceed your slowest effect — or use `heartbeat`. |
| `clock` | `() -> float` epoch seconds. For tests; production uses `time.time`. |

### `run(key, payload, fn, *, ttl_sec=None, lease_sec=None, wait=True, ...) -> Any`
The one-call form: `begin` → (if elected) `fn()` → `complete`/`fail`.
Returns the result — fresh or replayed, identical either way. Raises
`IdempotencyConflict`, `WaitTimeout`, or whatever `fn` raised
(after freeing the key for retry).

### `begin(key, payload, *, ttl_sec=None, lease_sec=None, wait=False, wait_timeout_sec=30.0, poll_sec=0.05) -> Outcome`
Election. `Outcome.execute=True` → you won: run the effect, then call
`complete` or `fail` with `Outcome.record.fence_token`.
`execute=False` → replay: use `Outcome.record.result` / `.error`.
`wait=True` coalesces onto an in-flight attempt instead of raising
`InProgressError`; the timeout is measured in REAL time (a frozen test
clock cannot disable it).

### `complete(key, fence_token, result, *, ttl_sec=None) -> Record`
Seal success. Only the holding fence may complete; a stale token raises
`RuntimeError("...fenced out...")` — that means another attempt took
over: do not retry, do not trust your result. Retrying `complete` with
the SAME token is safe (idempotent).

### `fail(key, fence_token, error, *, allow_retry=True, ttl_sec=None) -> None`
Record failure. `allow_retry=True` frees the key (a later attempt may
execute). `allow_retry=False` = terminal: future `begin`s replay the
error as `Status.FAILED`.

### `heartbeat(key, fence_token, *, lease_sec=None) -> bool`
Extend the lease mid-flight. `False` = you lost ownership (lease expired
and someone reclaimed): STOP working; your `complete` will be fenced out.

---

## Stores

### `MemoryStore(clock=None)`
Single-process, mutex-linearized. Perfect for one worker or tests.
State dies with the process.

### `PostgresStore(conninfo, *, table="once_records", clock=None, min_size=1, max_size=10, create_schema=True)`
Multi-process/multi-box production store (`pip install "once-kernel[postgres]"`).
Every mutation is ONE conditional SQL statement — the database's
atomicity is the referee. Schema auto-creates; or run
`PostgresStore.schema_sql(table)` in your migrations. Helpers:
`close()`, `drop_table()` (test benches only).

### Writing your own (`Store` protocol)
Implement `get`, `create_in_progress`, `cas_complete`, `cas_fail`,
`reclaim_if_lease_dead`, `heartbeat` — each atomic and conditional
(compare-and-set semantics). Run the chaos suite against it via the
`once_factory` fixture pattern in `tests/conftest.py`; a store that
passes that suite inherits every guarantee in the README.

---

## `AsyncOnce` — asyncio façade

```python
AsyncOnce(once=None, **once_kwargs)
```

Same five operations, `await`-able: `begin / complete / fail /
heartbeat / run`. Two hard guarantees (both enforced by tests):
a **sync** `fn` passed to `run` executes in a worker thread — the event
loop never stalls; **waiters park on the event loop**, never inside the
thread pool — duplicate storms cannot starve the executor the winner
needs. `run` accepts sync or `async` callables.

---

## MCP server — `once-mcp`

`pip install "once-kernel[mcp]"`, then `claude mcp add once -- once-mcp`.
Tools: `once_begin` / `once_complete` / `once_fail` / `once_heartbeat`
(statuses: `fresh`, `already_done`, `in_flight`, `conflict`,
`failed_previously`, `wait_timeout`, `extended`, `lost`).
Env `ONCE_PG_DSN` switches the ledger to PostgreSQL for cross-process
state. In-memory otherwise (per server process).

---

## Data types

**`Outcome`** — `execute: bool` · `record: Record` · `replayed: bool`

**`Record`** — `key` · `payload_hash` · `status` (`Status.IN_PROGRESS/COMPLETED/FAILED`) ·
`fence_token` · `generation` (monotonic, +1 per takeover — export it to
downstream systems for `WHERE generation >= $seen` fencing) · `result` ·
`error` · `created_at` / `updated_at` / `expires_at` / `lease_expires_at`

**`IdempotencyKey`** — validated key wrapper (non-empty, ≤256 chars).
Plain `str` is accepted everywhere.

---

## Exceptions

| Exception | Meaning | Your move |
|---|---|---|
| `IdempotencyConflict` | Same key, different payload | Bug or attack — investigate; never retry blindly |
| `InProgressError` | Live lease held elsewhere (`retry_after_ms` attached) | Wait and re-`begin`, or use `wait=True` |
| `WaitTimeout` | `wait=True` patience exhausted | Surface to caller; the action may still complete |
| `ResultTooLarge` | Result exceeds the size policy | Store a pointer (id/URL), not the blob |
| `RuntimeError` ("fenced out") | Your lease was taken over | STOP; another attempt owns the action |

---

## Canonicalization contract

`payload_hash = SHA-256( RFC 8785 JCS bytes )` — implemented by the
[`rfc8785`](https://pypi.org/project/rfc8785/) package, not hand-rolled
rules. Ports to other languages MUST conform to RFC 8785 for
cross-language key+payload stability. Payloads must be JSON-shaped
(no NaN, no datetimes — pre-serialize them).
