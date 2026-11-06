"""Asyncio TCP cache server and a ring-aware client for a local demo.

The protocol is one JSON object per line. Servers bind to loopback only.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from .hashing import HashRing
from .node import CacheNode

LOOPBACK = "127.0.0.1"


class CacheServer:
    def __init__(self, node: CacheNode) -> None:
        self.node = node
        self.port = 0
        self._server: asyncio.base_events.Server | None = None

    async def start(self, port: int = 0) -> None:
        self._server = await asyncio.start_server(self._handle, LOOPBACK, port)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while line := await reader.readline():
                reply = self._dispatch(json.loads(line))
                writer.write(json.dumps(reply).encode() + b"\n")
                await writer.drain()
        except (ConnectionError, json.JSONDecodeError):
            pass
        finally:
            writer.close()

    def _dispatch(self, request: dict[str, Any]) -> dict[str, Any]:
        op, key = request.get("op"), request.get("key", "")
        if op == "get":
            entry = self.node.get(key)
            if entry is None:
                return {"ok": True, "found": False}
            return {"ok": True, "found": True, "value": entry.value, "version": entry.version}
        if op == "set":
            stored = self.node.set(key, request["value"], request["version"], request.get("ttl"))
            return {"ok": stored}
        if op == "delete":
            return {"ok": self.node.delete(key)}
        return {"ok": False, "error": f"unknown op {op!r}"}


class AsyncClusterClient:
    """Routes requests over TCP with a hash ring and N-way replication."""

    def __init__(self, ports: dict[str, int], replication: int = 2, timeout: float = 1.0) -> None:
        self.ports = ports
        self.replication = replication
        self.timeout = timeout
        self.ring = HashRing(list(ports))

    async def _request(self, node: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(LOOPBACK, self.ports[node]), self.timeout
            )
        except (OSError, asyncio.TimeoutError):
            return None
        try:
            writer.write(json.dumps(payload).encode() + b"\n")
            await writer.drain()
            line = await asyncio.wait_for(reader.readline(), self.timeout)
            return json.loads(line) if line else None
        except (OSError, asyncio.TimeoutError, json.JSONDecodeError):
            return None
        finally:
            writer.close()

    async def set(self, key: str, value: Any, ttl: float | None = None) -> int:
        payload = {"op": "set", "key": key, "value": value, "version": time.time_ns(), "ttl": ttl}
        nodes = self.ring.get_nodes(key, self.replication)
        replies = await asyncio.gather(*(self._request(n, payload) for n in nodes))
        return sum(1 for r in replies if r and r.get("ok"))

    async def get(self, key: str) -> Any:
        nodes = self.ring.get_nodes(key, self.replication)
        replies = await asyncio.gather(*(self._request(n, {"op": "get", "key": key}) for n in nodes))
        found = [r for r in replies if r and r.get("found")]
        if not found:
            return None
        return max(found, key=lambda r: r["version"])["value"]


async def run_demo(keys: int = 30) -> dict[str, Any]:
    servers = {f"node-{i}": CacheServer(CacheNode(f"node-{i}", 1 << 20)) for i in range(3)}
    for server in servers.values():
        await server.start()
    client = AsyncClusterClient({name: s.port for name, s in servers.items()}, replication=2)
    try:
        for i in range(keys):
            await client.set(f"user:{i}", f"profile-{i}")
        per_node = {name: len(s.node) for name, s in servers.items()}
        await servers["node-1"].stop()
        values = [await client.get(f"user:{i}") for i in range(keys)]
        readable = sum(1 for i, v in enumerate(values) if v == f"profile-{i}")
    finally:
        for server in servers.values():
            await server.stop()
    return {
        "ports": client.ports,
        "keys_per_node": per_node,
        "stopped": "node-1",
        "readable_after_stop": readable,
        "total": keys,
    }


def main() -> None:
    result = asyncio.run(run_demo())
    print(f"started 3 servers on {LOOPBACK}: {result['ports']}")
    print(f"keys per node (replication=2): {result['keys_per_node']}")
    print(f"stopped {result['stopped']}")
    print(f"readable after stop: {result['readable_after_stop']}/{result['total']}")


if __name__ == "__main__":
    main()
