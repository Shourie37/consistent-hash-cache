import threading
import time

from chcache import SingleFlight


def run_concurrently(n: int, fn) -> list:
    barrier = threading.Barrier(n)
    results: list = []
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        try:
            value = fn()
        except Exception as exc:  # noqa: BLE001
            value = exc
        with lock:
            results.append(value)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


def test_concurrent_callers_share_one_execution() -> None:
    flight = SingleFlight()
    calls = []

    def slow() -> int:
        calls.append(1)
        time.sleep(0.2)
        return 42

    results = run_concurrently(50, lambda: flight.do("k", slow))
    assert results == [42] * 50
    assert len(calls) == 1
    assert flight.shared == 49


def test_distinct_keys_run_separately() -> None:
    flight = SingleFlight()
    assert flight.do("a", lambda: 1) == 1
    assert flight.do("b", lambda: 2) == 2
    assert flight.executions == 2


def test_error_is_shared_and_key_is_released() -> None:
    flight = SingleFlight()

    def boom() -> None:
        time.sleep(0.2)
        raise RuntimeError("backend down")

    results = run_concurrently(10, lambda: flight.do("k", boom))
    assert all(isinstance(r, RuntimeError) for r in results)
    assert flight.executions == 1
    assert flight.do("k", lambda: "ok") == "ok"


def test_sequential_calls_each_execute() -> None:
    flight = SingleFlight()
    flight.do("k", lambda: 1)
    flight.do("k", lambda: 1)
    assert flight.executions == 2
