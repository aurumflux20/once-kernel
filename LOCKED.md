# once — architecture lock (2026-08-04)

## Adopted without argument
- Postgres-first (only durable store pre-launch)
- Fence-correctness as CI gate
- Heartbeat leases
- `wait=True` coalescing
- Honest model: **exactly-once execution + at-least-once delivery**
- Tier-4 avoid list (no agent OS, no Temporal clone, no dashboard-first)
- Kill dates on incomplete work
- **Dogfood-or-archive:** `nova_mailer` wraps sends so retries never double-email

## Pushbacks — LOCKED
| # | Decision |
|---|----------|
| 1 | **Redis is Tier 1 / v0.3+.** Not pre-launch. Failover can lose acknowledged records; money truth stays in Postgres. |
| 2 | **Multi-key transactions: GONE.** Not optional. That is a saga engine. |
| 3 | **Monotonic `generation` int** on every claim/reclaim. Fence token = our CAS equality; generation = export fence for downstream `WHERE generation >= $x`. |
| 4 | **Payload canonicalization = RFC 8785 (JCS).** No hand-rolled number/unicode rules. |

## Missed items — now mandatory pre-launch
- **Async-first API** (FastAPI / agents)
- **Result-size + PII policy** documented page

## Dogfood
`aurumflux-api/nova_mailer.py` → `send_approved_card` path: key = `mail:card:{decision_id}` (or stronger), payload = stable card fingerprint; never double Resend on retry.
