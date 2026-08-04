# Changelog

## [Unreleased]
- Roadmap: PyPI publish + public repo · README storm GIF · API reference page · Redis store (v0.3) · outbox hook (v0.3)

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
