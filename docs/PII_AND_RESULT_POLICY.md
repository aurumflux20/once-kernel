# Result size & PII policy

Security reviewers ask two questions. This page answers both.

## What we store

| Field | Contents | PII risk |
|-------|----------|----------|
| `key` | Caller-chosen idempotency key | **Your problem** — do not put raw email/SSN in the key; use opaque IDs (`mail:card:{uuid}`) |
| `payload_hash` | SHA-256 of RFC 8785 canonical payload | **Not reversible** — hash only, not the body |
| `result` | JSON-serializable outcome of the side effect | **Can contain PII** if you put it there |
| `error` | Short error string | Usually low; avoid dumping bodies |
| `fence_token` / `generation` | Opaque fencing | None |

**`once` does not log raw payloads by default.** The kernel only persists `payload_hash`, not the original payload bytes.

## Result size limits

| Limit | Default | Rationale |
|-------|---------|-----------|
| Max result JSON size | **64 KiB** (65_536 bytes UTF-8) | Prevents the idempotency table becoming a blob store |
| Max error string | **2 KiB** | Enough for reason codes, not stack dumps of secrets |

Oversized results → `ResultTooLarge` at `complete()` time. **Store a pointer** (object key, message id) in `result`, not the full email body.

## Recommended patterns

**Emails (nova_mailer dogfood):**
```text
key     = mail:card:{decision_id}
payload = { "decision_id", "to_hash", "template_id", "product" }  # no full body
result  = { "resend_message_id", "sent": true }                 # no message text
```

**Payments:**
```text
payload = { "amount", "currency", "customer_id" }
result  = { "charge_id" }
```

## Retention

- Default TTL: 24h (configurable).
- Completed rows are not a CRM. Delete or expire aggressively.
- Enterprise: set TTL to match your dispute window (e.g. 7–30 days), not forever.

## Compliance stance

- `once` is infrastructure, not a system of record for personal data.
- If `result` may contain PII, your DPA/retention policy applies to the **store** (Postgres), not to this library’s algorithm.
- Prefer **hashes and foreign keys** in stored results.
