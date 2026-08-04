"""
once.kernel — the correctness core.

Side effects under retries must run once. Concurrent first requests must elect
one executor. Same key + different payload must hard-fail (never silent reuse).

Honest model: exactly-once *execution* + at-least-once *delivery* (clients retry;
we replay COMPLETED results).

Storage is pluggable. Pre-launch durable store = Postgres only (Redis = v0.3+).
"""
from __future__ import annotations

import hashlib
import secrets
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Protocol, runtime_checkable

from once.canonical import canonical_bytes, payload_hash_hex


# ---------------------------------------------------------------------------
# Canonical fingerprinting — RFC 8785 JCS (see once.canonical)
# ---------------------------------------------------------------------------

def canonical_json(obj: Any) -> bytes:
    """RFC 8785 JCS bytes. Name kept for call-site clarity."""
    return canonical_bytes(obj)


def payload_hash(payload: Any) -> str:
    """
    H(payload) = hex(SHA-256( RFC8785(payload) )).
    Same key + same hash ⇒ same operation. Cross-language ports must use JCS.
    """
    return payload_hash_hex(payload)


def fingerprint(key: str, payload: Any) -> str:
    """Composite id for logs/metrics — not used as storage primary key alone."""
    return hashlib.sha256(f"{key}\0{payload_hash(payload)}".encode()).hexdigest()


# Default result cap (see docs/PII_AND_RESULT_POLICY.md)
MAX_RESULT_BYTES = 65_536
MAX_ERROR_BYTES = 2_048


# ---------------------------------------------------------------------------
# Domain model
# ---------------------------------------------------------------------------

class Status(str, Enum):
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"  # terminal soft-fail: allow retry with SAME payload only


@dataclass(frozen=True, slots=True)
class IdempotencyKey:
    """Caller-supplied key. Scope (tenant/route) is the caller's job to prefix."""

    value: str

    def __post_init__(self) -> None:
        v = self.value.strip()
        if not v:
            raise ValueError("idempotency key must be non-empty")
        if len(v) > 256:
            raise ValueError("idempotency key max length 256")
        object.__setattr__(self, "value", v)


@dataclass(slots=True)
class Record:
    key: str
    payload_hash: str
    status: Status
    fence_token: str
    result: Any = None
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    lease_expires_at: Optional[float] = None  # IN_PROGRESS lease; reclaim if dead
    generation: int = 1
    """Monotonic per-key fencing generation. Starts at 1; +1 on every reclaim
    (and on a fresh claim over an expired record). Downstream systems that
    accept writes from workers can store the generation they last saw and
    reject anything lower (`WHERE generation >= $seen`) — classic fencing for
    storage we don't control. The random `fence_token` remains the kernel's
    own equality check; `generation` is the export-grade ordering."""

    def is_expired(self, now: Optional[float] = None) -> bool:
        now = now if now is not None else time.time()
        return self.expires_at is not None and now >= self.expires_at

    def lease_dead(self, now: Optional[float] = None) -> bool:
        now = now if now is not None else time.time()
        if self.status != Status.IN_PROGRESS:
            return False
        if self.lease_expires_at is None:
            return False
        return now >= self.lease_expires_at


class IdempotencyConflict(Exception):
    """Same key, different payload — production must not guess which body wins."""

    def __init__(self, key: str, existing_hash: str, attempted_hash: str):
        self.key = key
        self.existing_hash = existing_hash
        self.attempted_hash = attempted_hash
        super().__init__(
            f"idempotency conflict on key={key!r}: "
            f"stored payload_hash={existing_hash[:12]}… "
            f"request payload_hash={attempted_hash[:12]}…"
        )


# Back-compat alias
ConflictError = IdempotencyConflict


class InProgressError(Exception):
    """Another worker holds a live lease. Client should wait/retry (not re-execute)."""

    def __init__(self, key: str, retry_after_ms: int = 100):
        self.key = key
        self.retry_after_ms = retry_after_ms
        super().__init__(f"idempotency in progress key={key!r} retry_after_ms={retry_after_ms}")


class ResultTooLarge(ValueError):
    """Result exceeds MAX_RESULT_BYTES — store a pointer, not a blob."""


class WaitTimeout(TimeoutError):
    """wait=True coalescing timed out before COMPLETED/FAILED."""


@dataclass(frozen=True, slots=True)
class Outcome:
    """
    What begin() returns.

    execute=True  → you are the elected worker; run the side effect, then complete/fail.
    execute=False → side effect already done (or failed terminal); use result / error.
    """

    execute: bool
    record: Record
    replayed: bool  # True if COMPLETED/FAILED returned without new work


# ---------------------------------------------------------------------------
# Store protocol — adapters must provide linearizable conditional ops
# ---------------------------------------------------------------------------

@runtime_checkable
class Store(Protocol):
    def get(self, key: str) -> Optional[Record]:
        ...

    def create_in_progress(
        self,
        key: str,
        payload_hash: str,
        fence_token: str,
        *,
        ttl_sec: Optional[float],
        lease_sec: float,
    ) -> bool:
        """
        Atomic: insert IN_PROGRESS only if key absent (or expired/reclaimable).
        Return True if this caller won the insert.
        """
        ...

    def cas_complete(
        self,
        key: str,
        fence_token: str,
        result: Any,
        *,
        ttl_sec: Optional[float],
    ) -> bool:
        """Complete only if still IN_PROGRESS and fence_token matches."""
        ...

    def cas_fail(
        self,
        key: str,
        fence_token: str,
        error: str,
        *,
        ttl_sec: Optional[float],
        allow_retry: bool,
    ) -> bool:
        """
        If allow_retry: delete or mark FAILED reclaimable so same payload can retry.
        If not: terminal FAILED with stored error.
        """
        ...

    def reclaim_if_lease_dead(
        self,
        key: str,
        payload_hash: str,
        new_fence_token: str,
        *,
        lease_sec: float,
        ttl_sec: Optional[float],
    ) -> bool:
        """
        If record is IN_PROGRESS with dead lease AND same payload_hash,
        take over with new fence_token and generation+1. Else False.
        """
        ...

    def heartbeat(
        self,
        key: str,
        fence_token: str,
        *,
        lease_sec: float,
    ) -> bool:
        """Extend lease only if IN_PROGRESS and fence_token matches."""
        ...


class MemoryStore:
    """
    Single-process linearizable store (threading.Lock = mutex for the map).
    Production: Postgres (INSERT … ON CONFLICT DO NOTHING + UPDATE … WHERE fence).
    Redis is intentionally not pre-launch (see LOCKED.md).

    `clock` is injectable so lease/TTL behavior is testable without real
    sleeps. Pass the SAME clock to `Once` and its store, or time will split.
    """

    def __init__(self, clock: Optional[Callable[[], float]] = None) -> None:
        self._mu = threading.RLock()
        self._rows: dict[str, Record] = {}
        self._clock: Callable[[], float] = clock or time.time

    def get(self, key: str) -> Optional[Record]:
        with self._mu:
            rec = self._rows.get(key)
            if rec is None:
                return None
            if rec.is_expired(self._clock()):
                del self._rows[key]
                return None
            return rec

    def create_in_progress(
        self,
        key: str,
        payload_hash: str,
        fence_token: str,
        *,
        ttl_sec: Optional[float],
        lease_sec: float,
    ) -> bool:
        with self._mu:
            now = self._clock()
            cur = self._rows.get(key)
            if cur is not None and not cur.is_expired(now):
                return False
            exp = (now + ttl_sec) if ttl_sec is not None else None
            gen = 1
            if cur is not None and cur.is_expired(now):
                gen = cur.generation + 1
            self._rows[key] = Record(
                key=key,
                payload_hash=payload_hash,
                status=Status.IN_PROGRESS,
                fence_token=fence_token,
                created_at=now,
                updated_at=now,
                expires_at=exp,
                lease_expires_at=now + lease_sec,
                generation=gen,
            )
            return True

    def cas_complete(
        self,
        key: str,
        fence_token: str,
        result: Any,
        *,
        ttl_sec: Optional[float],
    ) -> bool:
        with self._mu:
            now = self._clock()
            cur = self._rows.get(key)
            if cur is None or cur.is_expired(now):
                return False
            if cur.status != Status.IN_PROGRESS or cur.fence_token != fence_token:
                return False
            exp = (now + ttl_sec) if ttl_sec is not None else cur.expires_at
            self._rows[key] = Record(
                key=key,
                payload_hash=cur.payload_hash,
                status=Status.COMPLETED,
                fence_token=cur.fence_token,
                result=result,
                created_at=cur.created_at,
                updated_at=now,
                expires_at=exp,
                lease_expires_at=None,
                generation=cur.generation,
            )
            return True

    def cas_fail(
        self,
        key: str,
        fence_token: str,
        error: str,
        *,
        ttl_sec: Optional[float],
        allow_retry: bool,
    ) -> bool:
        with self._mu:
            now = self._clock()
            cur = self._rows.get(key)
            if cur is None or cur.is_expired(now):
                return False
            if cur.status != Status.IN_PROGRESS or cur.fence_token != fence_token:
                return False
            if allow_retry:
                # Free the key so the same payload can begin again.
                del self._rows[key]
                return True
            exp = (now + ttl_sec) if ttl_sec is not None else cur.expires_at
            self._rows[key] = Record(
                key=key,
                payload_hash=cur.payload_hash,
                status=Status.FAILED,
                fence_token=cur.fence_token,
                error=error,
                created_at=cur.created_at,
                updated_at=now,
                expires_at=exp,
                lease_expires_at=None,
                generation=cur.generation,
            )
            return True

    def reclaim_if_lease_dead(
        self,
        key: str,
        payload_hash: str,
        new_fence_token: str,
        *,
        lease_sec: float,
        ttl_sec: Optional[float],
    ) -> bool:
        with self._mu:
            now = self._clock()
            cur = self._rows.get(key)
            if cur is None or cur.is_expired(now):
                return False
            if cur.status != Status.IN_PROGRESS:
                return False
            if not cur.lease_dead(now):
                return False
            if cur.payload_hash != payload_hash:
                return False
            exp = (now + ttl_sec) if ttl_sec is not None else cur.expires_at
            self._rows[key] = Record(
                key=key,
                payload_hash=payload_hash,
                status=Status.IN_PROGRESS,
                fence_token=new_fence_token,
                created_at=cur.created_at,
                updated_at=now,
                expires_at=exp,
                lease_expires_at=now + lease_sec,
                generation=cur.generation + 1,
            )
            return True

    def heartbeat(
        self,
        key: str,
        fence_token: str,
        *,
        lease_sec: float,
    ) -> bool:
        with self._mu:
            now = self._clock()
            cur = self._rows.get(key)
            if cur is None or cur.is_expired(now):
                return False
            if cur.status != Status.IN_PROGRESS or cur.fence_token != fence_token:
                return False
            self._rows[key] = Record(
                key=key,
                payload_hash=cur.payload_hash,
                status=Status.IN_PROGRESS,
                fence_token=cur.fence_token,
                result=cur.result,
                error=cur.error,
                created_at=cur.created_at,
                updated_at=now,
                expires_at=cur.expires_at,
                lease_expires_at=now + lease_sec,
                generation=cur.generation,
            )
            return True


# ---------------------------------------------------------------------------
# Kernel
# ---------------------------------------------------------------------------

class Once:
    """
    Enterprise idempotency kernel.

    Algorithm (begin):
      ph = H(payload)
      1. load record by key
      2. if missing → try create IN_PROGRESS with fence_token (CAS insert)
         win → execute; lose → re-load and follow (3–6)
      3. if payload_hash mismatch → CONFLICT (never return other body's result)
      4. if COMPLETED → replay result (execute=False)
      5. if FAILED terminal → replay error (execute=False)  [or policy]
      6. if IN_PROGRESS live lease → InProgressError (wait)
      7. if IN_PROGRESS dead lease + same hash → reclaim CAS → execute

    Algorithm (complete):
      CAS status IN_PROGRESS→COMPLETED only if fence_token matches.
      Stale workers after reclaim cannot complete (fenced out).

    This is the part seniors get wrong: missing fence tokens after reclaim
    double-applies side effects when a slow worker finishes late.
    """

    def __init__(
        self,
        store: Optional[Store] = None,
        *,
        default_ttl_sec: Optional[float] = 24 * 3600,
        lease_sec: float = 30.0,
        clock: Optional[Callable[[], float]] = None,
    ):
        self.store: Store = store if store is not None else MemoryStore()
        self.default_ttl_sec = default_ttl_sec
        self.lease_sec = lease_sec
        self._clock = clock or time.time

    def begin(
        self,
        key: str | IdempotencyKey,
        payload: Any,
        *,
        ttl_sec: Optional[float] = None,
        lease_sec: Optional[float] = None,
        wait: bool = False,
        wait_timeout_sec: float = 30.0,
        poll_sec: float = 0.05,
    ) -> Outcome:
        """
        Elect executor or replay.

        wait=True: if another worker holds a live lease, poll until COMPLETED/FAILED
        (or we reclaim / win), then return — coalescing without re-executing.
        """
        key_s = key.value if isinstance(key, IdempotencyKey) else IdempotencyKey(key).value
        ph = payload_hash(payload)
        ttl = self.default_ttl_sec if ttl_sec is None else ttl_sec
        lease = self.lease_sec if lease_sec is None else lease_sec
        # Caller patience is REAL elapsed time (monotonic), deliberately NOT the
        # injected clock: leases/TTLs are simulated time, but a blocked caller
        # burns real seconds. Mixing the two made the timeout unfireable under
        # a frozen test clock (and under any custom clock in production).
        deadline_mono = time.monotonic() + wait_timeout_sec if wait else None

        while True:
            # Fast path: empty → try become leader
            fence = _new_fence_token()
            if self.store.create_in_progress(key_s, ph, fence, ttl_sec=ttl, lease_sec=lease):
                rec = self.store.get(key_s)
                assert rec is not None
                return Outcome(execute=True, record=rec, replayed=False)

            try:
                return self._resolve_existing(key_s, ph, ttl=ttl, lease=lease)
            except InProgressError:
                if not wait:
                    raise
                if deadline_mono is not None and time.monotonic() >= deadline_mono:
                    raise WaitTimeout(f"once: wait timed out key={key_s!r}")
                time.sleep(poll_sec)

    def _resolve_existing(
        self,
        key_s: str,
        ph: str,
        *,
        ttl: Optional[float],
        lease: float,
    ) -> Outcome:
        rec = self.store.get(key_s)
        if rec is None:
            # raced with expiry — retry insert once
            fence = _new_fence_token()
            if self.store.create_in_progress(key_s, ph, fence, ttl_sec=ttl, lease_sec=lease):
                rec2 = self.store.get(key_s)
                assert rec2 is not None
                return Outcome(execute=True, record=rec2, replayed=False)
            rec = self.store.get(key_s)
            if rec is None:
                raise RuntimeError("once: store unstable under concurrent begin")

        # Payload mismatch is always fatal for this key
        if rec.payload_hash != ph:
            raise IdempotencyConflict(key_s, rec.payload_hash, ph)

        if rec.status == Status.COMPLETED:
            return Outcome(execute=False, record=rec, replayed=True)

        if rec.status == Status.FAILED:
            # Terminal failure with same payload: replay (caller decides UX)
            return Outcome(execute=False, record=rec, replayed=True)

        # IN_PROGRESS
        if rec.lease_dead(self._clock()):
            fence = _new_fence_token()
            if self.store.reclaim_if_lease_dead(
                key_s, ph, fence, lease_sec=lease, ttl_sec=ttl
            ):
                rec2 = self.store.get(key_s)
                assert rec2 is not None
                return Outcome(execute=True, record=rec2, replayed=False)
            # lost reclaim race — re-read authoritative state
            rec = self.store.get(key_s)
            if rec is None:
                fence = _new_fence_token()
                if self.store.create_in_progress(
                    key_s, ph, fence, ttl_sec=ttl, lease_sec=lease
                ):
                    rec2 = self.store.get(key_s)
                    assert rec2 is not None
                    return Outcome(execute=True, record=rec2, replayed=False)
                rec = self.store.get(key_s)
                if rec is None:
                    raise RuntimeError("once: store unstable after reclaim race")
            if rec.payload_hash != ph:
                raise IdempotencyConflict(key_s, rec.payload_hash, ph)
            if rec.status == Status.COMPLETED:
                return Outcome(execute=False, record=rec, replayed=True)
            if rec.status == Status.FAILED:
                return Outcome(execute=False, record=rec, replayed=True)

        # Live lease held by someone else
        retry_ms = 100
        if rec.lease_expires_at:
            retry_ms = max(50, int((rec.lease_expires_at - self._clock()) * 1000))
        raise InProgressError(key_s, retry_after_ms=min(retry_ms, int(lease * 1000)))

    def heartbeat(
        self,
        key: str | IdempotencyKey,
        fence_token: str,
        *,
        lease_sec: Optional[float] = None,
    ) -> bool:
        """Extend the IN_PROGRESS lease. Call from long tools so reclaim does not steal."""
        key_s = key.value if isinstance(key, IdempotencyKey) else IdempotencyKey(key).value
        lease = self.lease_sec if lease_sec is None else lease_sec
        return self.store.heartbeat(key_s, fence_token, lease_sec=lease)

    def complete(
        self,
        key: str | IdempotencyKey,
        fence_token: str,
        result: Any,
        *,
        ttl_sec: Optional[float] = None,
    ) -> Record:
        key_s = key.value if isinstance(key, IdempotencyKey) else IdempotencyKey(key).value
        ttl = self.default_ttl_sec if ttl_sec is None else ttl_sec
        _assert_result_size(result)
        ok = self.store.cas_complete(key_s, fence_token, result, ttl_sec=ttl)
        if not ok:
            # Only the holding fence may complete. Same-token retry of complete is OK.
            rec = self.store.get(key_s)
            if (
                rec
                and rec.status == Status.COMPLETED
                and rec.fence_token == fence_token
            ):
                return rec
            raise RuntimeError(
                f"once: complete fenced out key={key_s!r} "
                f"(stale worker or lost lease) token={fence_token[:8]}…"
            )
        rec = self.store.get(key_s)
        assert rec is not None
        return rec

    def fail(
        self,
        key: str | IdempotencyKey,
        fence_token: str,
        error: str,
        *,
        allow_retry: bool = True,
        ttl_sec: Optional[float] = None,
    ) -> None:
        key_s = key.value if isinstance(key, IdempotencyKey) else IdempotencyKey(key).value
        ttl = self.default_ttl_sec if ttl_sec is None else ttl_sec
        ok = self.store.cas_fail(
            key_s, fence_token, error, ttl_sec=ttl, allow_retry=allow_retry
        )
        if not ok:
            raise RuntimeError(f"once: fail fenced out key={key_s!r}")

    def run(
        self,
        key: str | IdempotencyKey,
        payload: Any,
        fn: Callable[[], Any],
        *,
        ttl_sec: Optional[float] = None,
        lease_sec: Optional[float] = None,
        wait: bool = True,
        wait_timeout_sec: float = 30.0,
        retry_in_progress: int = 8,
        retry_sleep_sec: float = 0.05,
    ) -> Any:
        """
        High-level: begin → if execute, fn() → complete/fail.
        Default wait=True coalesces concurrent callers onto one execution.
        """
        if wait:
            out = self.begin(
                key,
                payload,
                ttl_sec=ttl_sec,
                lease_sec=lease_sec,
                wait=True,
                wait_timeout_sec=wait_timeout_sec,
                poll_sec=retry_sleep_sec,
            )
            if not out.execute:
                if out.record.status == Status.FAILED:
                    raise RuntimeError(out.record.error or "idempotent call previously failed")
                return out.record.result
            try:
                result = fn()
            except Exception as e:
                self.fail(key, out.record.fence_token, str(e), allow_retry=True)
                raise
            self.complete(key, out.record.fence_token, result, ttl_sec=ttl_sec)
            return result

        last_err: Optional[Exception] = None
        for _ in range(max(1, retry_in_progress)):
            try:
                out = self.begin(key, payload, ttl_sec=ttl_sec, lease_sec=lease_sec, wait=False)
            except InProgressError as e:
                last_err = e
                time.sleep(retry_sleep_sec)
                continue

            if not out.execute:
                if out.record.status == Status.FAILED:
                    raise RuntimeError(out.record.error or "idempotent call previously failed")
                return out.record.result

            try:
                result = fn()
            except Exception as e:
                self.fail(key, out.record.fence_token, str(e), allow_retry=True)
                raise

            self.complete(key, out.record.fence_token, result, ttl_sec=ttl_sec)
            return result

        assert last_err is not None
        raise last_err


def _assert_result_size(result: Any) -> None:
    raw = canonical_bytes(result)
    if len(raw) > MAX_RESULT_BYTES:
        raise ResultTooLarge(
            f"result is {len(raw)} bytes; max {MAX_RESULT_BYTES}. "
            "Store a pointer (e.g. message_id), not the full body."
        )


def _new_fence_token() -> str:
    # Unpredictable + unique: reclaim cannot be guessed; CAS is the real fence.
    return f"{uuid.uuid4().hex}{secrets.token_hex(8)}"
