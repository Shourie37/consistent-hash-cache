"""A single cache node with LRU or LFU eviction, TTL, and a byte budget."""

from __future__ import annotations

import time
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable

Clock = Callable[[], float]


@dataclass
class Entry:
    value: Any
    version: int
    expires_at: float | None = None
    # Seconds the loader took; probabilistic early expiration uses it.
    delta: float = 0.0
    size: int = 0

    def is_expired(self, now: float) -> bool:
        return self.expires_at is not None and now >= self.expires_at


def sizeof(key: str, value: Any) -> int:
    """Approximate the bytes an item costs against the node budget."""
    if isinstance(value, (bytes, bytearray)):
        payload = len(value)
    elif isinstance(value, str):
        payload = len(value.encode())
    else:
        payload = len(repr(value).encode())
    return len(key.encode()) + payload


class LRUPolicy:
    def __init__(self) -> None:
        self._order: OrderedDict[str, None] = OrderedDict()

    def add(self, key: str) -> None:
        self._order[key] = None

    def touch(self, key: str) -> None:
        self._order.move_to_end(key)

    def remove(self, key: str) -> None:
        self._order.pop(key, None)

    def victim(self) -> str:
        return next(iter(self._order))


class LFUPolicy:
    """O(1) LFU. Ties within a frequency break by least recent use."""

    def __init__(self) -> None:
        self._freq: dict[str, int] = {}
        self._buckets: defaultdict[int, OrderedDict[str, None]] = defaultdict(OrderedDict)
        self._min_freq = 0

    def add(self, key: str) -> None:
        self._freq[key] = 1
        self._buckets[1][key] = None
        self._min_freq = 1

    def touch(self, key: str) -> None:
        freq = self._freq[key]
        bucket = self._buckets[freq]
        del bucket[key]
        if not bucket:
            del self._buckets[freq]
            if self._min_freq == freq:
                self._min_freq = freq + 1
        self._freq[key] = freq + 1
        self._buckets[freq + 1][key] = None

    def remove(self, key: str) -> None:
        freq = self._freq.pop(key, None)
        if freq is None:
            return
        bucket = self._buckets[freq]
        del bucket[key]
        if not bucket:
            del self._buckets[freq]
            if self._min_freq == freq and self._buckets:
                self._min_freq = min(self._buckets)

    def victim(self) -> str:
        return next(iter(self._buckets[self._min_freq]))


POLICIES = {"lru": LRUPolicy, "lfu": LFUPolicy}


@dataclass
class NodeStats:
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    rejected: int = 0

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total else 0.0


@dataclass
class CacheNode:
    name: str
    capacity_bytes: int
    policy: str = "lru"
    clock: Clock = time.monotonic
    stats: NodeStats = field(default_factory=NodeStats)

    def __post_init__(self) -> None:
        if self.policy not in POLICIES:
            raise ValueError(f"unknown policy {self.policy!r}")
        self._policy = POLICIES[self.policy]()
        self._data: dict[str, Entry] = {}
        self.used_bytes = 0

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, key: str) -> bool:
        return self.peek(key) is not None

    def keys(self) -> list[str]:
        return list(self._data)

    def peek(self, key: str) -> Entry | None:
        """Read without changing eviction order or stats."""
        entry = self._data.get(key)
        if entry is None or entry.is_expired(self.clock()):
            return None
        return entry

    def get(self, key: str) -> Entry | None:
        entry = self._data.get(key)
        if entry is not None and entry.is_expired(self.clock()):
            self._drop(key)
            self.stats.expirations += 1
            entry = None
        if entry is None:
            self.stats.misses += 1
            return None
        self.stats.hits += 1
        self._policy.touch(key)
        return entry

    def set(
        self,
        key: str,
        value: Any,
        version: int,
        ttl: float | None = None,
        delta: float = 0.0,
    ) -> bool:
        """Store a value. Return False when the item is larger than the node."""
        size = sizeof(key, value)
        if size > self.capacity_bytes:
            self.stats.rejected += 1
            return False
        if key in self._data:
            self._drop(key)
        expires_at = self.clock() + ttl if ttl is not None else None
        while self.used_bytes + size > self.capacity_bytes:
            self._evict()
        self._data[key] = Entry(value, version, expires_at, delta, size)
        self._policy.add(key)
        self.used_bytes += size
        return True

    def put_entry(self, key: str, entry: Entry) -> bool:
        """Store a replica entry as-is, keeping its version and absolute expiry."""
        now = self.clock()
        if entry.is_expired(now):
            return False
        ttl = entry.expires_at - now if entry.expires_at is not None else None
        return self.set(key, entry.value, entry.version, ttl, entry.delta)

    def delete(self, key: str) -> bool:
        if key not in self._data:
            return False
        self._drop(key)
        return True

    def purge_expired(self) -> int:
        now = self.clock()
        expired = [k for k, e in self._data.items() if e.is_expired(now)]
        for key in expired:
            self._drop(key)
        self.stats.expirations += len(expired)
        return len(expired)

    def _evict(self) -> None:
        self._drop(self._policy.victim())
        self.stats.evictions += 1

    def _drop(self, key: str) -> None:
        entry = self._data.pop(key)
        self._policy.remove(key)
        self.used_bytes -= entry.size
