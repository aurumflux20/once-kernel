"""
MCP server integration tests — real subprocess, real JSON-RPC over stdio.

The kernel's own idempotency is the lie detector here (same trick as the
effectfence wrap tests): once_begin with the same key is answered
differently by a *fresh* execution ("fresh") versus the recorded state
("in_flight"/"already_done"), so response content proves which path ran.

Skips cleanly when the `mcp` extra isn't installed.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys

import pytest

pytest.importorskip("mcp", reason="mcp extra not installed")


class Server:
    def __init__(self) -> None:
        exe = shutil.which("once-mcp")
        if exe is None:
            pytest.skip("once-mcp entrypoint not on PATH (install with pip install -e .[mcp])")
        self.proc = subprocess.Popen(
            [exe],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        self.next_id = 1
        init = self.request(
            "initialize",
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "ci", "version": "0"},
            },
        )
        assert init["result"]["serverInfo"]["name"] == "once", init
        self.notify("notifications/initialized")

    def send(self, msg: dict) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(msg) + "\n")
        self.proc.stdin.flush()

    def notify(self, method: str) -> None:
        self.send({"jsonrpc": "2.0", "method": method})

    def request(self, method: str, params: dict) -> dict:
        rid = self.next_id
        self.next_id += 1
        self.send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
        assert self.proc.stdout is not None
        while True:
            line = self.proc.stdout.readline()
            assert line, f"server closed stdout awaiting id {rid}"
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if msg.get("id") == rid:
                return msg

    def call(self, tool: str, args: dict) -> dict:
        resp = self.request("tools/call", {"name": tool, "arguments": args})
        text = resp["result"]["content"][0]["text"]
        return json.loads(text)

    def close(self) -> None:
        if self.proc.stdin:
            self.proc.stdin.close()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()


@pytest.fixture()
def server():
    s = Server()
    yield s
    s.close()


def test_tools_are_exposed(server):
    resp = server.request("tools/list", {})
    names = {t["name"] for t in resp["result"]["tools"]}
    assert {"once_begin", "once_complete", "once_fail", "once_heartbeat"} <= names


def test_full_lifecycle_over_the_wire(server):
    # fresh -> in_flight -> completed -> already_done replay -> conflict
    b1 = server.call("once_begin", {"key": "pay:1", "payload": {"amt": 49}})
    assert b1["status"] == "fresh", b1

    b2 = server.call("once_begin", {"key": "pay:1", "payload": {"amt": 49}, "wait": False})
    assert b2["status"] == "in_flight", b2

    c = server.call(
        "once_complete",
        {"key": "pay:1", "fence_token": b1["fence_token"], "result": {"charge_id": "ch_1"}},
    )
    assert c["status"] == "completed", c

    b3 = server.call("once_begin", {"key": "pay:1", "payload": {"amt": 49}})
    assert b3["status"] == "already_done" and b3["result"] == {"charge_id": "ch_1"}, b3

    b4 = server.call("once_begin", {"key": "pay:1", "payload": {"amt": 9900}})
    assert b4["status"] == "conflict", b4


def test_fail_and_heartbeat_paths(server):
    b = server.call("once_begin", {"key": "job:hb", "payload": {"x": 1}})
    assert b["status"] == "fresh"

    hb = server.call("once_heartbeat", {"key": "job:hb", "fence_token": b["fence_token"]})
    assert hb["still_owner"] is True, hb

    f = server.call(
        "once_fail",
        {"key": "job:hb", "fence_token": b["fence_token"], "error": "boom", "allow_retry": True},
    )
    assert f["status"] == "failed_recorded" and f["retryable"] is True, f

    retry = server.call("once_begin", {"key": "job:hb", "payload": {"x": 1}})
    assert retry["status"] == "fresh", "retryable failure must free the key"

    # A zombie heartbeat with the OLD token must be rejected.
    zombie = server.call("once_heartbeat", {"key": "job:hb", "fence_token": b["fence_token"]})
    assert zombie["still_owner"] is False, zombie
