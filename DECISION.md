# Decision lock — 2026-08-04

## Name
**once** (package: `once-kernel` on PyPI to avoid name collisions)

## Wedge (one sentence)
Enterprise idempotency kernel so backend and AI-tool engineers never double-apply side effects under retries, redelivery, or concurrent workers — same key + different payload hard-fails; fence tokens stop stale workers after lease reclaim.

## Kill list
- FactChain (simple hash ledger)
- EffectFence as v1 flagship (defer as optional concurrency layer)
- Agent-memory / RAG products

## Bar
Nova-level correctness. 10k-star-*worthy* quality. Stars are a byproduct.

## Architecture lock (see LOCKED.md)
- Postgres-only pre-launch; Redis v0.3+
- No multi-key / saga
- Monotonic generation + fence_token
- RFC 8785 JCS for payload hash
- Async API mandatory
- PII + result-size policy documented
- Dogfood: nova_mailer double-email prevention

## Next
1. Wire dogfood into aurumflux-api nova_mailer (deploy path)
2. Postgres CI against real DB
3. Claude MCP wrap
4. Illegal-transition matrix as formal tests doc complete
