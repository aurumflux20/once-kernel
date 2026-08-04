"""
Payload canonicalization — RFC 8785 JSON Canonicalization Scheme (JCS).

We do NOT hand-roll number/unicode rules. Cross-language ports MUST cite and
conform to the same RFC so payload_hash is stable across Python/Node/Go.
"""
from __future__ import annotations

from typing import Any

import rfc8785


def canonical_bytes(payload: Any) -> bytes:
    """Return RFC 8785 JCS bytes for `payload`."""
    return rfc8785.dumps(payload)


def payload_hash_hex(payload: Any) -> str:
    import hashlib

    return hashlib.sha256(canonical_bytes(payload)).hexdigest()
