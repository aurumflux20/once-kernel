# Changelog

## [Unreleased]

**Docs — the README no longer says an unknown outcome never re-runs.**

`run()` frees the key whenever your function raises, a timeout included,
and a lease takeover after a worker dies runs the function again. The
README now says so and points to `fail(..., allow_retry=False)` for the
case where the effect may already have landed. No code change.

**Fixed — a failed commit could re-run an effect that already ran.**

`run()` only ever converted an exception from *your function* into a released
reservation. If the function succeeded but recording the result failed, the
record was left `IN_PROGRESS` — so the lease expired, the next caller
reclaimed the key, and the side effect fired a **second** time. Reachable on
an ordinary mistake: `ResultTooLarge` is a documented, first-class exception,
and every durable store can also refuse a value at write time.

A commit that cannot be recorded is now **terminal**: the attempt is marked
`FAILED` with `allow_retry=False`, later callers replay that failure instead
of re-executing, and the caller still sees the original error. Recovering
means a new idempotency key — the choice to run an already-run effect again
belongs to the caller, not to a lease timeout. Applies to `Once.run()` and
`AsyncOnce.run()`.

Also: cleanup after a failure no longer masks the real error. A zombie
worker's release CAS legitimately fails because it is fenced out, and that
bookkeeping error used to replace the declined card / oversized result the
caller actually needed to see.

- Roadmap: Redis store (v0.3) · outbox hook (v0.3)

## [0.2.0] — 2026-08-05

**`SqliteStore` — durability without a database server.**

`MemoryStore` dies with the process and protects nothing once a second
worker exists; `PostgresStore` protects everything but wants a server. Most
single-box deployments sat in the gap between them. `SqliteStore` closes it:
durable across restarts, safe across processes (WAL mode), and it needs
nothing installed — `sqlite3` is in the standard library.

It is held to the same bar as the other stores rather than trusted: the
shared chaos bench in `conftest.py` is now parametrized over
memory / **sqlite** / postgres, so every forced-race, thread-storm,
zombie-fencing and dead-lease-reclaim test attacks SQLite identically.
Plus `test_sqlite_durability.py` for the claim the shared bench cannot
make — that a completed effect is still recorded after the process dies,
and that a worker which crashed mid-effect leaves a reclaimable lease
rather than a wedged key. 65 tests, 0 skipped.

Also in this release:
- `examples/` — FastAPI webhook and Celery task, documenting the three
  decisions that actually take judgement (key, payload, store).
- Fixed `once.__version__` drifting behind `pyproject.toml`, and added a
  test so it cannot drift again.
- Ownership marker for the official MCP Registry listing.

## [0.1.0] — 2026-08-04

First complete core. Everything below is enforced by the 45-test suite —
thread storms, reclaim races, zombie fencing, frozen-clock attacks —
running against BOTH MemoryStore and real PostgreSQL in CI (which fails
loudly if the Postgres bench is skipped).

### Added
- **Kernel**: payload fingerprinting (RFC 8785), atomic leader election,
  lease reclaim with fence tokens + monotonic generations, terminal vs
  retryable failure semantics, result-size guard, `wait=True` coalescing,
  `heartbeat()` lease renewal.
- **PostgresStore**: every mutation a single conditional SQL statement;
  injectable clock; pooled; schema auto-create.
- **AsyncOnce**: sync effects dispatched to worker threads (event loop
  never stalls — enforced by a pulse-counting test); waiters park on the
  event loop (no executor starvation under duplicate storms).
- **MCP server** (`once-mcp`): once_begin / once_complete / once_fail /
  once_heartbeat for any MCP agent; `ONCE_PG_DSN` for shared state.
- **Storm demo** (`python -m once.demo`): 1,000 concurrent duplicate
  charges → 1 execution, enforced in CI.
- Dogfood: production mailer guard (concurrent double-approve cannot
  double-send) — live in aurumflux-api since 2026-08-04.

### Fixed during pre-launch audit (each with a regression test)
- Decorative test clock (stores secretly used wall time)
- Wait deadline measured on a freezable clock — timeout could never fire
- Async façade ran sync side effects ON the event loop
- Waiter thread-pool starvation deadlock under duplicate storms
- Dogfood wrapper rebuilt an amnesiac kernel per call
- Raw recipient email persisted against the PII policy
