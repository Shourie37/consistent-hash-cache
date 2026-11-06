"""Key placement strategies: consistent hash ring, modulo, and rendezvous."""

from __future__ import annotations

import bisect
import hashlib
from typing import Protocol


def stable_hash(text: str) -> int:
    """Return a 64-bit hash that is the same across processes and runs."""
    digest = hashlib.blake2b(text.encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big")


class Placement(Protocol):
    """Maps a key to an ordered list of distinct nodes."""

    @property
    def nodes(self) -> list[str]: ...

    def add_node(self, node: str) -> None: ...

    def remove_node(self, node: str) -> None: ...

    def get_nodes(self, key: str, count: int) -> list[str]: ...

    def get_node(self, key: str) -> str: ...


class HashRing:
    """Consistent hash ring. Each node owns `vnodes` points on the ring."""

    def __init__(self, nodes: list[str] | None = None, vnodes: int = 160) -> None:
        if vnodes < 1:
            raise ValueError("vnodes must be >= 1")
        self.vnodes = vnodes
        self._points: list[int] = []
        self._owners: list[str] = []
        self._nodes: set[str] = set()
        for node in nodes or []:
            self.add_node(node)

    @property
    def nodes(self) -> list[str]:
        return sorted(self._nodes)

    def add_node(self, node: str) -> None:
        if node in self._nodes:
            return
        self._nodes.add(node)
        for i in range(self.vnodes):
            point = stable_hash(f"{node}#{i}")
            idx = bisect.bisect_left(self._points, point)
            self._points.insert(idx, point)
            self._owners.insert(idx, node)

    def remove_node(self, node: str) -> None:
        if node not in self._nodes:
            return
        self._nodes.remove(node)
        keep = [(p, o) for p, o in zip(self._points, self._owners) if o != node]
        self._points = [p for p, _ in keep]
        self._owners = [o for _, o in keep]

    def get_nodes(self, key: str, count: int) -> list[str]:
        """Walk clockwise from the key and collect `count` distinct nodes."""
        if not self._points:
            raise LookupError("ring is empty")
        count = min(count, len(self._nodes))
        start = bisect.bisect_right(self._points, stable_hash(key))
        found: list[str] = []
        size = len(self._points)
        for step in range(size):
            owner = self._owners[(start + step) % size]
            if owner not in found:
                found.append(owner)
                if len(found) == count:
                    break
        return found

    def get_node(self, key: str) -> str:
        return self.get_nodes(key, 1)[0]


class ModuloHasher:
    """Naive `hash(key) % N` placement. Used as a baseline."""

    def __init__(self, nodes: list[str] | None = None) -> None:
        self._nodes: list[str] = sorted(nodes or [])

    @property
    def nodes(self) -> list[str]:
        return list(self._nodes)

    def add_node(self, node: str) -> None:
        if node not in self._nodes:
            self._nodes.append(node)
            self._nodes.sort()

    def remove_node(self, node: str) -> None:
        if node in self._nodes:
            self._nodes.remove(node)

    def get_nodes(self, key: str, count: int) -> list[str]:
        if not self._nodes:
            raise LookupError("no nodes")
        count = min(count, len(self._nodes))
        first = stable_hash(key) % len(self._nodes)
        return [self._nodes[(first + i) % len(self._nodes)] for i in range(count)]

    def get_node(self, key: str) -> str:
        return self.get_nodes(key, 1)[0]


class RendezvousHasher:
    """Highest-random-weight hashing: each key picks the nodes with top scores."""

    def __init__(self, nodes: list[str] | None = None) -> None:
        self._nodes: set[str] = set(nodes or [])

    @property
    def nodes(self) -> list[str]:
        return sorted(self._nodes)

    def add_node(self, node: str) -> None:
        self._nodes.add(node)

    def remove_node(self, node: str) -> None:
        self._nodes.discard(node)

    def get_nodes(self, key: str, count: int) -> list[str]:
        if not self._nodes:
            raise LookupError("no nodes")
        ranked = sorted(self._nodes, key=lambda n: stable_hash(f"{n}|{key}"), reverse=True)
        return ranked[:count]

    def get_node(self, key: str) -> str:
        return self.get_nodes(key, 1)[0]


def remap_fraction(before: Placement, after: Placement, keys: list[str]) -> float:
    """Return the share of keys whose primary node differs between two placements."""
    moved = sum(1 for k in keys if before.get_node(k) != after.get_node(k))
    return moved / len(keys)
