"""
Async-first façade for FastAPI / AI agents.

Uses the same Store protocol. Store calls run in a worker thread
(`asyncio.to_thread`) so the event loop never blocks on lock contention or
database round trips — and, critically, neither does a *sync* side-effect
callable passed to `run` (it is dispatched to a thread too; an async
callable is awaited natively).

Postgres adapter can grow a native-async twin later; until then this is
correct, just not zero-thread.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable, Optional, Union

from once.kernel import (
    IdempotencyKey,
    InProgressError,
    Once,
    Outcome,
    Record,
    Status,
    WaitTimeout,
)


class AsyncOnce:
    def __init__(self, once: Optional[Once] = None, **once_kwargs: Any) -> None:
        self._once = once if once is not None else Once(**once_kwargs)

    @property
    def sync(self) -> Once:
        return self._once

    async def begin(
        self,
        key: Union[str, IdempotencyKey],
        payload: Any,
        *,
        wait: bool = False,
        wait_timeout_sec: float = 30.0,
        poll_sec: float = 0.05,
        **kwargs: Any,
    ) -> Outcome:
        """
        wait=True waits ON THE EVENT LOOP (`asyncio.sleep`), never inside a
        worker thread. This matters: parking waiters in `to_thread` holds the
        bounded default executor hostage, and enough concurrent duplicates
        starve the very thread the winner needs to finish — a coalescing
        deadlock. Threads are used only for short store round trips.
        """
        if not wait:
            return await asyncio.to_thread(self._once.begin, key, payload, **kwargs)

        deadline = time.monotonic() + wait_timeout_sec
        while True:
            try:
                return await asyncio.to_thread(
                    self._once.begin, key, payload, **kwargs
                )
            except InProgressError:
                if time.monotonic() >= deadline:
                    raise WaitTimeout(f"once: wait timed out key={key!r}") from None
                await asyncio.sleep(poll_sec)

    async def complete(
        self,
        key: Union[str, IdempotencyKey],
        fence_token: str,
        result: Any,
        **kwargs: Any,
    ) -> Record:
        return await asyncio.to_thread(
            self._once.complete, key, fence_token, result, **kwargs
        )

    async def fail(
        self,
        key: Union[str, IdempotencyKey],
        fence_token: str,
        error: str,
        **kwargs: Any,
    ) -> None:
        await asyncio.to_thread(self._once.fail, key, fence_token, error, **kwargs)

    async def heartbeat(
        self,
        key: Union[str, IdempotencyKey],
        fence_token: str,
        **kwargs: Any,
    ) -> bool:
        return await asyncio.to_thread(self._once.heartbeat, key, fence_token, **kwargs)

    async def run(
        self,
        key: Union[str, IdempotencyKey],
        payload: Any,
        fn: Union[Callable[[], Any], Callable[[], Awaitable[Any]]],
        *,
        ttl_sec: Optional[float] = None,
        lease_sec: Optional[float] = None,
        wait: bool = True,
        wait_timeout_sec: float = 30.0,
        poll_sec: float = 0.05,
    ) -> Any:
        """
        Like Once.run, `fn` may be sync OR async.

        Semantics match the sync API: wait=True (default) coalesces onto an
        in-flight attempt instead of erroring. A sync `fn` is executed in a
        worker thread so it can never stall the event loop; an async `fn`
        is awaited natively. Heartbeat remains the caller's job for
        multi-minute tools.
        """
        out = await self.begin(
            key,
            payload,
            wait=wait,
            wait_timeout_sec=wait_timeout_sec,
            poll_sec=poll_sec,
            ttl_sec=ttl_sec,
            lease_sec=lease_sec,
        )
        if not out.execute:
            if out.record.status == Status.FAILED:
                raise RuntimeError(out.record.error or "idempotent call previously failed")
            return out.record.result

        try:
            if asyncio.iscoroutinefunction(fn):
                result = await fn()
            else:
                result = await asyncio.to_thread(fn)
                # A sync callable may still hand back an awaitable.
                if asyncio.iscoroutine(result):
                    result = await result
        except Exception as e:
            await asyncio.to_thread(
                self._once._best_effort_fail,
                key, out.record.fence_token, str(e),
                ttl=ttl_sec, allow_retry=True,
            )
            raise

        # Commit through the sync kernel's `_commit`, so a result that cannot
        # be stored lands in a TERMINAL state here too. Leaving it IN_PROGRESS
        # would let the lease die and a reclaimer re-run an effect that already
        # ran — see Once._commit.
        return await asyncio.to_thread(
            self._once._commit, key, out.record.fence_token, result, ttl_sec=ttl_sec
        )
