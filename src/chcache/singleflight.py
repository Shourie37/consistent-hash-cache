"""Request coalescing: concurrent callers for one key share one load."""

from __future__ import annotations

import threading
from typing import Any, Callable


class _Call:
    def __init__(self) -> None:
        self.done = threading.Event()
        self.result: Any = None
        self.error: BaseException | None = None
        self.waiters = 0


class SingleFlight:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._calls: dict[str, _Call] = {}
        self.executions = 0
        self.shared = 0

    def do(self, key: str, fn: Callable[[], Any]) -> Any:
        with self._lock:
            call = self._calls.get(key)
            leader = call is None
            if leader:
                call = self._calls[key] = _Call()
                self.executions += 1
            else:
                call.waiters += 1
                self.shared += 1
        if not leader:
            call.done.wait()
            if call.error is not None:
                raise call.error
            return call.result
        try:
            call.result = fn()
        except BaseException as exc:
            call.error = exc
            raise
        finally:
            with self._lock:
                del self._calls[key]
            call.done.set()
        return call.result
