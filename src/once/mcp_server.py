"""
once MCP server — "do it once" as tools any MCP agent can call.

Agents are double-fire machines: retries, re-plans, and parallel branches
re-invoke the same side-effecting tool. Put `once` between the agent and
the effect:

    once_begin  -> "fresh" (you won: run the effect, then report back)
                   "already_done" (here's the stored result -- do NOT run)
                   "in_flight" / "conflict" / "failed_previously" (do NOT run)
    once_complete / once_fail -> report the outcome
    once_heartbeat            -> keep a long effect's lease alive

State: in-memory per server process by default; set ONCE_PG_DSN to share
the ledger across processes/restarts via PostgreSQL.

Install & wire:
    pip install "once-kernel[mcp]"          # (+ [postgres] for ONCE_PG_DSN)
    claude mcp add once -- once-mcp
"""
from __future__ import annotations

import os
from typing import Any, Optional

from mcp.server.mcpserver import MCPServer

from once.kernel import (
    IdempotencyConflict,
    InProgressError,
    Once,
    Status,
    WaitTimeout,
)

mcp = MCPServer(
    name="once",
    instructions=(
        "Idempotency guard for side-effecting tools (payments, emails, "
        "provisioning). BEFORE running such a tool, call once_begin with a "
        "stable key for the logical action (e.g. 'pay:order-123') and the "
        "action's payload. Only run the tool on status='fresh'; then report "
        "with once_complete (success) or once_fail (failure). On "
        "status='already_done', use the returned result -- do NOT run the "
        "tool again. Same key with a different payload is refused "
        "(status='conflict') -- never guess which body wins."
    ),
)

_kernel: Optional[Once] = None


def _get_kernel() -> Once:
    global _kernel
    if _kernel is None:
        dsn = os.environ.get("ONCE_PG_DSN", "").strip()
        if dsn:
            from once.pg import PostgresStore

            _kernel = Once(PostgresStore(dsn))
        else:
            _kernel = Once()
    return _kernel


@mcp.tool()
def once_begin(
    key: str,
    payload: dict,
    wait: bool = True,
    wait_timeout_sec: float = 25.0,
) -> dict:
    """Ask permission to run a side-effecting action exactly once.

    key: stable id of the LOGICAL action ('pay:order-123'). Same action
    retried -> same key. Genuinely new action -> new key.
    payload: the action's parameters; guards against key reuse.
    Returns status: 'fresh' (run it, keep fence_token) | 'already_done'
    (use result, do NOT run) | 'in_flight' | 'conflict' |
    'failed_previously' | 'wait_timeout' (all: do NOT run).
    """
    o = _get_kernel()
    try:
        out = o.begin(key, payload, wait=wait, wait_timeout_sec=wait_timeout_sec)
    except IdempotencyConflict as e:
        return {"status": "conflict", "detail": str(e)}
    except InProgressError as e:
        return {"status": "in_flight", "retry_after_ms": e.retry_after_ms}
    except WaitTimeout:
        return {"status": "wait_timeout"}

    if out.execute:
        return {"status": "fresh", "fence_token": out.record.fence_token}
    if out.record.status == Status.FAILED:
        return {"status": "failed_previously", "error": out.record.error}
    return {"status": "already_done", "result": out.record.result}


@mcp.tool()
def once_complete(key: str, fence_token: str, result: dict) -> dict:
    """Report that the action succeeded; stores `result` for future replays.

    Only the fence_token from this attempt's once_begin may complete it —
    a stale worker's token is rejected (that rejection means another
    attempt took over; do not retry)."""
    o = _get_kernel()
    try:
        rec = o.complete(key, fence_token, result)
    except Exception as e:  # fenced out / infra — the caller must not guess
        return {"status": "rejected", "detail": str(e)}
    return {"status": "completed", "generation": rec.generation}


@mcp.tool()
def once_fail(key: str, fence_token: str, error: str, allow_retry: bool = True) -> dict:
    """Report that the action failed. allow_retry=True frees the key so a
    later attempt may execute; allow_retry=False records a terminal
    failure that future once_begin calls will replay as
    'failed_previously'."""
    o = _get_kernel()
    try:
        o.fail(key, fence_token, error, allow_retry=allow_retry)
    except Exception as e:
        return {"status": "rejected", "detail": str(e)}
    return {"status": "failed_recorded", "retryable": allow_retry}


@mcp.tool()
def once_heartbeat(key: str, fence_token: str) -> dict:
    """Extend the lease while a long action is still genuinely running,
    so no other attempt takes it over mid-flight. Call every ~15s for
    multi-minute work. False means the lease is no longer yours —
    STOP the work; another attempt owns the action now."""
    o = _get_kernel()
    ok = o.heartbeat(key, fence_token)
    return {"status": "extended" if ok else "lost", "still_owner": ok}


def main() -> None:
    mcp.run()  # stdio transport


if __name__ == "__main__":
    main()
