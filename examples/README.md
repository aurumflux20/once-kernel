# Examples

Working integrations, each one a single `once.run()` call in a real setting.

| File | Shows |
|---|---|
| [`fastapi_webhook.py`](fastapi_webhook.py) | A webhook receiver that survives at-least-once redelivery and concurrent retries. |
| [`celery_task.py`](celery_task.py) | A task that stays exactly-once across many worker processes, using `PostgresStore`. |

## The three decisions the examples are really about

Wiring `once` in is one line. These are the parts that take judgement, and both files are commented around them.

**1. What is the key?** The key defines what counts as "the same operation". Use the provider's event id, the order id — something stable across retries. Never a Celery task id or a timestamp, because a retry gets a new one and the guard silently stops working.

**2. What goes in the payload?** The payload is fingerprinted, so a key reused with different content hard-fails instead of quietly doing the wrong thing. Leave out anything that legitimately varies between retries of the same operation — delivery counters, receive timestamps — or every retry looks like a conflict.

**3. Which store?** `MemoryStore` is per-process. It is fine for one web process and worthless the moment you run two, because each keeps its own private idea of what already ran. `SqliteStore` is durable and safe across processes on one machine, and needs nothing installed. `PostgresStore` is the arbiter once the effect can run on more than one machine.

| Store | Survives restart | Multi-process | Multi-machine | Needs a server |
|---|---|---|---|---|
| `MemoryStore` | no | no | no | no |
| `SqliteStore` | yes | yes (one box) | no | no |
| `PostgresStore` | yes | yes | yes | yes |
