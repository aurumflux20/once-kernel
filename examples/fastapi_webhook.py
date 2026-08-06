"""Stripe-style webhook receiver that survives redelivery.

Providers deliver at-least-once: the same event will arrive twice, and a
retry can land while the first delivery is still being processed. Both
cases are handled here by one `once.run()` call.

    pip install once-kernel fastapi uvicorn
    uvicorn examples.fastapi_webhook:app --reload

Then send the same event twice -- the fulfilment runs once:

    curl -X POST localhost:8000/webhook -H 'content-type: application/json' \
      -d '{"id":"evt_123","type":"checkout.completed","amount":4900}'
"""

from typing import Any

from fastapi import FastAPI
from once import Once
from once.kernel import IdempotencyConflict

app = FastAPI()

# One process: MemoryStore is enough. Run more than one worker and the
# guard silently stops working -- swap in PostgresStore (see
# examples/celery_task.py) the moment you scale past a single process.
once = Once()


def fulfil_order(event: dict[str, Any]) -> dict[str, Any]:
    """The side effect. Charging, emailing, provisioning -- the thing you
    do not want to happen twice."""
    print(f"FULFILLING {event['id']} for {event['amount']}")
    return {"fulfilled": event["id"]}


@app.post("/webhook")
def webhook(event: dict[str, Any]) -> dict[str, Any]:
    # The key is what makes two deliveries "the same operation". The
    # provider's event id is the right choice -- not a timestamp, not a
    # hash of the whole body (which changes when they add a field).
    key = f"webhook:{event['id']}"

    # The payload is what gets fingerprinted. Exclude anything that
    # varies between redeliveries of the same event (delivery attempt
    # counters, receive timestamps) or every retry looks like a
    # different payload and hard-fails.
    payload = {"type": event["type"], "amount": event.get("amount")}

    try:
        result = once.run(key, payload, lambda: fulfil_order(event))
    except IdempotencyConflict:
        # Same event id, genuinely different content. Never silently
        # accept this -- it means the provider changed history or you
        # picked the wrong key.
        return {"status": "conflict", "event": event["id"]}

    # Note there is no branch here for "was this the first delivery or a
    # replay". Both return the same result, which is the point.
    return {"status": "ok", "result": result}
