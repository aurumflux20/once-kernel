"""
Dogfood integration for nova_mailer.

Wraps a send callable so retries/double-approve never double-email.

Key:    mail:card:{decision_id}
Payload (no full body — PII policy): decision_id, to_hash, product, subject_hash
Result: { sent, resend_message_id?, reason? }

Usage from aurumflux-api (after installing once-kernel on PYTHONPATH):

    from once.dogfood_mailer import send_card_once
    return send_card_once(decision, send_fn=lambda: legacy_send_approved_card(decision))
"""
from __future__ import annotations

import hashlib
from typing import Any, Callable, Optional

from once.kernel import Once


def _sha16(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def mail_idempotency_key(decision_id: Any) -> str:
    return f"mail:card:{decision_id}"


def mail_payload(decision: dict, card: Optional[dict] = None) -> dict:
    """Stable, low-PII fingerprint of the send intent."""
    to = ""
    subject = ""
    product = ""
    if card:
        to = str(card.get("to") or "")
        subject = str(card.get("subject") or "")
        product = str(card.get("product") or "")
    meta = decision.get("meta") or {}
    if not product:
        product = str(meta.get("product") or decision.get("product") or "")
    return {
        "decision_id": str(decision.get("id") or ""),
        "to_hash": _sha16(to) if to else _sha16(str(meta.get("to") or "")),
        "subject_hash": _sha16(subject) if subject else "0",
        "product": product,
    }


# Module-level default: ONE kernel per process, so repeat calls actually
# share a memory. (A fresh Once() per call would remember nothing — every
# call would look like the first, which is idempotency theater.) For real
# multi-worker deployments, pass an `Once(PostgresStore(...))` explicitly.
_default_once: Optional[Once] = None


def _default() -> Once:
    global _default_once
    if _default_once is None:
        _default_once = Once()
    return _default_once


def send_card_once(
    decision: dict,
    send_fn: Callable[[], dict],
    *,
    once: Optional[Once] = None,
    card: Optional[dict] = None,
) -> dict:
    """
    Exactly-once *execution* of send_fn for this decision id.
    At-least-once delivery of the approve click still allowed — second click replays.

    NOTE: the default kernel is per-process memory — enough to kill
    double-click/retry duplicates inside one server. Cross-worker/restart
    safety requires passing a Postgres-backed Once.
    """
    o = once or _default()
    key = mail_idempotency_key(decision.get("id"))
    payload = mail_payload(decision, card)

    def _run() -> dict:
        r = send_fn()
        # Persist only a small, PII-free result: hash the recipient, never
        # store the raw address (matches docs/PII_AND_RESULT_POLICY.md).
        to = str(r.get("to") or "")
        return {
            "ok": bool(r.get("ok")),
            "sent": bool(r.get("sent")),
            "reason": r.get("reason"),
            "message_id": r.get("message_id"),
            "to_hash": _sha16(to) if to else None,
        }

    return o.run(key, payload, _run, wait=True)
