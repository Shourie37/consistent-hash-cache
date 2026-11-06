"""Hot-key detection with a count-min sketch over a tumbling time window."""

from __future__ import annotations

import time

from .hashing import stable_hash
from .node import Clock


class CountMinSketch:
    def __init__(self, width: int = 2048, depth: int = 4) -> None:
        self.width = width
        self.depth = depth
        self._rows = [[0] * width for _ in range(depth)]

    def _cells(self, key: str) -> list[int]:
        return [stable_hash(f"{row}:{key}") % self.width for row in range(self.depth)]

    def add(self, key: str) -> int:
        """Increment the key and return its new estimated count."""
        estimate = None
        for row, col in enumerate(self._cells(key)):
            self._rows[row][col] += 1
            value = self._rows[row][col]
            estimate = value if estimate is None else min(estimate, value)
        return estimate or 0

    def estimate(self, key: str) -> int:
        return min(self._rows[row][col] for row, col in enumerate(self._cells(key)))

    def clear(self) -> None:
        for row in self._rows:
            row[:] = [0] * self.width


class HotKeyDetector:
    """Flags a key as hot when it passes `threshold` reads in one window."""

    def __init__(
        self,
        threshold: int = 100,
        window_seconds: float = 1.0,
        clock: Clock = time.monotonic,
    ) -> None:
        self.threshold = threshold
        self.window_seconds = window_seconds
        self.clock = clock
        self._sketch = CountMinSketch()
        self._window_start = clock()

    def record(self, key: str) -> bool:
        now = self.clock()
        if now - self._window_start >= self.window_seconds:
            self._sketch.clear()
            self._window_start = now
        return self._sketch.add(key) >= self.threshold
