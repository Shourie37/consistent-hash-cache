"""In-process network that can fail and recover nodes."""

from __future__ import annotations

import random
from typing import Any

from .node import CacheNode


class NodeUnavailable(Exception):
    pass


class SimulatedNetwork:
    def __init__(self, drop_rate: float = 0.0, seed: int = 0) -> None:
        self.nodes: dict[str, CacheNode] = {}
        self.down: set[str] = set()
        self.drop_rate = drop_rate
        self.calls = 0
        self._rng = random.Random(seed)

    def register(self, node: CacheNode) -> None:
        self.nodes[node.name] = node

    def fail(self, name: str) -> None:
        self.down.add(name)

    def recover(self, name: str) -> None:
        self.down.discard(name)

    def is_up(self, name: str) -> bool:
        return name in self.nodes and name not in self.down

    def call(self, name: str, method: str, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1
        if not self.is_up(name):
            raise NodeUnavailable(name)
        if self.drop_rate and self._rng.random() < self.drop_rate:
            raise NodeUnavailable(f"{name} (dropped)")
        return getattr(self.nodes[name], method)(*args, **kwargs)
