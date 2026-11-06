"""Cluster client: replication, read repair, hinted handoff, hot keys, single-flight."""

from __future__ import annotations

import math
import random
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable

from .hashing import Placement
from .hotkeys import HotKeyDetector
from .network import NodeUnavailable, SimulatedNetwork
from .node import CacheNode, Clock, Entry
from .singleflight import SingleFlight


def should_refresh_early(entry: Entry, now: float, beta: float, rng: random.Random) -> bool:
    """XFetch: refresh before expiry with a chance that rises as expiry nears."""
    if entry.expires_at is None or entry.delta <= 0 or beta <= 0:
        return False
    # 1 - random() is in (0, 1], so log() is defined.
    return now - entry.delta * beta * math.log(1.0 - rng.random()) >= entry.expires_at


@dataclass
class ClusterStats:
    reads: int = 0
    writes: int = 0
    near_cache_hits: int = 0
    read_repairs: int = 0
    hinted_writes: int = 0
    hints_replayed: int = 0
    failed_writes: int = 0
    loads: int = 0
    early_refreshes: int = 0


class ClusterClient:
    def __init__(
        self,
        network: SimulatedNetwork,
        placement: Placement,
        replication: int = 3,
        read_repair: bool = True,
        hot_threshold: int = 50,
        hot_window_seconds: float = 1.0,
        near_cache_bytes: int = 64 * 1024,
        near_cache_ttl: float = 0.5,
        early_expiration_beta: float = 1.0,
        tombstone_ttl: float = 60.0,
        clock: Clock = time.monotonic,
        seed: int = 0,
    ) -> None:
        self.network = network
        self.placement = placement
        self.replication = replication
        self.read_repair = read_repair
        self.near_cache_ttl = near_cache_ttl
        self.early_expiration_beta = early_expiration_beta
        self.tombstone_ttl = tombstone_ttl
        self.clock = clock
        self.stats = ClusterStats()
        self.hot_keys = HotKeyDetector(hot_threshold, hot_window_seconds, clock)
        self.near_cache = CacheNode("near-cache", near_cache_bytes, "lru", clock)
        self.single_flight = SingleFlight()
        # intended owner -> {key: substitute node holding the write}
        self.hints: defaultdict[str, dict[str, str]] = defaultdict(dict)
        self._rng = random.Random(seed)
        self._version = 0
        self._lock = threading.Lock()

    # Membership

    def add_node(self, node: CacheNode) -> None:
        self.network.register(node)
        self.placement.add_node(node.name)

    def remove_node(self, name: str) -> None:
        self.placement.remove_node(name)
        self.hints.pop(name, None)

    # Placement

    def owners(self, key: str) -> list[str]:
        return self.placement.get_nodes(key, self.replication)

    def _targets(self, key: str) -> tuple[list[str], dict[str, str]]:
        """Return N healthy nodes for the key and a map of substitute -> owner."""
        order = self.placement.get_nodes(key, len(self.placement.nodes))
        owners = order[: self.replication]
        spares = [n for n in order[self.replication :] if self.network.is_up(n)]
        targets: list[str] = []
        stand_ins: dict[str, str] = {}
        for owner in owners:
            if self.network.is_up(owner):
                targets.append(owner)
            elif spares:
                spare = spares.pop(0)
                targets.append(spare)
                stand_ins[spare] = owner
        return targets, stand_ins

    def _next_version(self) -> int:
        with self._lock:
            self._version = max(self._version + 1, time.time_ns())
            return self._version

    # Writes

    def set(self, key: str, value: Any, ttl: float | None = None, delta: float = 0.0) -> int:
        """Write to N replicas. Return the number of replicas that stored it."""
        if value is None:
            raise ValueError("None is reserved for delete tombstones")
        return self._write(key, value, ttl, delta)

    def delete(self, key: str) -> int:
        # A tombstone stops read repair from bringing back an old value.
        return self._write(key, None, self.tombstone_ttl, 0.0)

    def _write(self, key: str, value: Any, ttl: float | None, delta: float) -> int:
        self.stats.writes += 1
        self.near_cache.delete(key)
        version = self._next_version()
        targets, stand_ins = self._targets(key)
        acks = 0
        for node in targets:
            try:
                if self.network.call(node, "set", key, value, version, ttl, delta):
                    acks += 1
            except NodeUnavailable:
                continue
            if node in stand_ins:
                self.hints[stand_ins[node]][key] = node
                self.stats.hinted_writes += 1
        if acks == 0:
            self.stats.failed_writes += 1
        return acks

    def replay_hints(self) -> int:
        """Push hinted writes to owners that are back up. Return the count moved."""
        moved = 0
        for owner in list(self.hints):
            if not self.network.is_up(owner):
                continue
            pending = self.hints.pop(owner)
            for key, spare in pending.items():
                moved += self._hand_off(key, owner, spare)
        self.stats.hints_replayed += moved
        return moved

    def _hand_off(self, key: str, owner: str, spare: str) -> int:
        try:
            entry = self.network.call(spare, "peek", key)
        except NodeUnavailable:
            self.hints[owner][key] = spare
            return 0
        if entry is None:
            return 0
        current = self.network.call(owner, "peek", key)
        if current is None or current.version < entry.version:
            self.network.call(owner, "put_entry", key, entry)
        if spare not in self.owners(key):
            self.network.call(spare, "delete", key)
        return 1

    # Reads

    def get(self, key: str) -> Any:
        entry = self._get_entry(key)
        return None if entry is None else entry.value

    def _get_entry(self, key: str) -> Entry | None:
        self.stats.reads += 1
        hot = self.hot_keys.record(key)
        if hot:
            local = self.near_cache.get(key)
            if local is not None:
                self.stats.near_cache_hits += 1
                return local
        entry = self._read_replicas(key)
        if entry is None or entry.value is None:
            return None
        if hot:
            self._fill_near_cache(key, entry)
        return entry

    def _read_replicas(self, key: str) -> Entry | None:
        targets, _ = self._targets(key)
        replies: dict[str, Entry | None] = {}
        for node in targets:
            try:
                replies[node] = self.network.call(node, "get", key)
            except NodeUnavailable:
                continue
        found = [e for e in replies.values() if e is not None]
        if not found:
            return None
        latest = max(found, key=lambda e: e.version)
        if self.read_repair:
            self._repair(key, latest, replies)
        return latest

    def _repair(self, key: str, latest: Entry, replies: dict[str, Entry | None]) -> None:
        for node, entry in replies.items():
            if entry is None or entry.version < latest.version:
                try:
                    if self.network.call(node, "put_entry", key, latest):
                        self.stats.read_repairs += 1
                except NodeUnavailable:
                    continue

    def _fill_near_cache(self, key: str, entry: Entry) -> None:
        ttl = self.near_cache_ttl
        if entry.expires_at is not None:
            ttl = min(ttl, entry.expires_at - self.clock())
        if ttl > 0:
            self.near_cache.set(key, entry.value, entry.version, ttl, entry.delta)

    # Cache-aside with stampede protection

    def get_or_load(self, key: str, loader: Callable[[], Any], ttl: float | None = None) -> Any:
        entry = self._get_entry(key)
        if entry is not None:
            if not should_refresh_early(entry, self.clock(), self.early_expiration_beta, self._rng):
                return entry.value
            self.stats.early_refreshes += 1
        return self.single_flight.do(key, lambda: self._load(key, loader, ttl))

    def _load(self, key: str, loader: Callable[[], Any], ttl: float | None) -> Any:
        self.stats.loads += 1
        start = self.clock()
        value = loader()
        self.set(key, value, ttl, delta=self.clock() - start)
        return value
