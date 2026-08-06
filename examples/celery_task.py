"""Celery task that stays exactly-once across workers and retries.

Celery is at-least-once by design: a task redelivered after a worker
dies, or retried on failure, runs its body again. Across several worker
processes an in-memory guard is no guard at all, so this example uses
PostgresStore -- the same row is the arbiter for every worker on every
box.

    pip install once-kernel celery psycopg[binary]
    celery -A examples.celery_task worker -l info
"""

import os

from celery import Celery
from once import Once
from once.pg import PostgresStore

celery_app = Celery("examples", broker=os.environ.get("BROKER_URL", "redis://localhost:6379/0"))

# Multi-process, multi-box: the store must be shared. This is the line
# that MemoryStore cannot do -- with several workers, each process would
# keep its own private idea of what had already run.
once = Once(store=PostgresStore(os.environ["DATABASE_URL"]))


def charge_customer(order_id: str, amount_cents: int) -> dict[str, int | str]:
    print(f"CHARGING {order_id} {amount_cents}")
    return {"order": order_id, "charged": amount_cents}


@celery_app.task(bind=True, max_retries=5)
def charge_order(self, order_id: str, amount_cents: int) -> dict[str, int | str]:
    # Key on the order, not on the Celery task id: a retry gets a new
    # task id, so keying on it would defeat the whole guard.
    key = f"charge:{order_id}"
    payload = {"amount_cents": amount_cents}

    # wait=True means a concurrent worker that finds this key in flight
    # blocks and receives the same result, instead of erroring or running
    # a second charge.
    return once.run(key, payload, lambda: charge_customer(order_id, amount_cents), wait=True)
