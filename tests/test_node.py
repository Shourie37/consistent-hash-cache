import pytest

from chcache import CacheNode


def test_lru_evicts_least_recently_used() -> None:
    node = CacheNode("n", capacity_bytes=6, policy="lru")
    node.set("a", "1", 1)
    node.set("b", "2", 1)
    node.set("c", "3", 1)
    node.get("a")
    node.set("d", "4", 1)
    assert "b" not in node
    assert {"a", "c", "d"} <= set(node.keys())
    assert node.stats.evictions == 1


def test_lfu_evicts_least_frequently_used() -> None:
    node = CacheNode("n", capacity_bytes=6, policy="lfu")
    node.set("a", "1", 1)
    node.set("b", "2", 1)
    node.set("c", "3", 1)
    for _ in range(3):
        node.get("a")
    node.get("b")
    node.set("d", "4", 1)
    assert "c" not in node
    assert {"a", "b", "d"} <= set(node.keys())


def test_lfu_breaks_ties_by_recency() -> None:
    node = CacheNode("n", capacity_bytes=4, policy="lfu")
    node.set("a", "1", 1)
    node.set("b", "2", 1)
    node.set("c", "3", 1)
    assert "a" not in node and "b" in node


def test_lfu_delete_keeps_min_frequency_valid() -> None:
    node = CacheNode("n", capacity_bytes=4, policy="lfu")
    node.set("a", "1", 1)
    node.set("b", "2", 1)
    node.get("b")
    node.delete("a")
    node.set("c", "3", 1)
    node.set("d", "4", 1)
    assert "b" in node and "c" not in node


def test_ttl_expires_lazily(clock) -> None:
    node = CacheNode("n", 100, clock=clock)
    node.set("k", "v", 1, ttl=5)
    clock.advance(4.9)
    assert node.get("k").value == "v"
    clock.advance(0.2)
    assert node.get("k") is None
    assert node.stats.expirations == 1
    assert node.used_bytes == 0


def test_purge_expired(clock) -> None:
    node = CacheNode("n", 100, clock=clock)
    node.set("a", "v", 1, ttl=1)
    node.set("b", "v", 1)
    clock.advance(2)
    assert node.purge_expired() == 1
    assert node.keys() == ["b"]


def test_byte_budget_is_enforced() -> None:
    node = CacheNode("n", capacity_bytes=1000)
    for i in range(100):
        node.set(f"k{i:03d}", b"x" * 96, 1)
        assert node.used_bytes <= 1000
    assert len(node) == 10


def test_overwrite_updates_size() -> None:
    node = CacheNode("n", 100)
    node.set("k", "x" * 50, 1)
    node.set("k", "x" * 10, 2)
    assert node.used_bytes == 11
    assert node.get("k").version == 2


def test_oversized_item_is_rejected() -> None:
    node = CacheNode("n", 10)
    assert node.set("k", "x" * 20, 1) is False
    assert node.stats.rejected == 1


def test_unknown_policy() -> None:
    with pytest.raises(ValueError):
        CacheNode("n", 10, policy="fifo")
