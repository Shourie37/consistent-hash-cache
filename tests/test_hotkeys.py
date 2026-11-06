from chcache import CountMinSketch, HotKeyDetector


def test_sketch_never_undercounts() -> None:
    sketch = CountMinSketch(width=64, depth=4)
    for i in range(500):
        sketch.add(f"k{i % 50}")
    assert all(sketch.estimate(f"k{i}") >= 10 for i in range(50))


def test_detector_flags_after_threshold(clock) -> None:
    detector = HotKeyDetector(threshold=5, window_seconds=1.0, clock=clock)
    flags = [detector.record("k") for _ in range(6)]
    assert flags == [False, False, False, False, True, True]


def test_detector_window_resets(clock) -> None:
    detector = HotKeyDetector(threshold=3, window_seconds=1.0, clock=clock)
    for _ in range(3):
        detector.record("k")
    clock.advance(1.1)
    assert detector.record("k") is False
