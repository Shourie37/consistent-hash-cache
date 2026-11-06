import statistics

import pytest

from chcache import HashRing, ModuloHasher, RendezvousHasher, remap_fraction

KEYS = [f"key-{i}" for i in range(20_000)]


def nodes(n: int) -> list[str]:
    return [f"node-{i}" for i in range(n)]


def test_placement_is_deterministic() -> None:
    a, b = HashRing(nodes(5)), HashRing(nodes(5))
    assert all(a.get_node(k) == b.get_node(k) for k in KEYS[:1000])


def test_get_nodes_returns_distinct_replicas() -> None:
    ring = HashRing(nodes(5))
    for key in KEYS[:500]:
        replicas = ring.get_nodes(key, 3)
        assert len(set(replicas)) == 3
        assert replicas[0] == ring.get_node(key)


def test_get_nodes_caps_at_cluster_size() -> None:
    assert len(HashRing(nodes(2)).get_nodes("k", 5)) == 2


def test_empty_ring_raises() -> None:
    with pytest.raises(LookupError):
        HashRing().get_node("k")


@pytest.mark.parametrize("n", [4, 8, 16])
def test_ring_add_moves_about_one_over_n(n: int) -> None:
    before, after = HashRing(nodes(n)), HashRing(nodes(n))
    after.add_node(f"node-{n}")
    ideal = 1 / (n + 1)
    assert remap_fraction(before, after, KEYS) == pytest.approx(ideal, rel=0.25)


def test_ring_add_only_moves_keys_to_the_new_node() -> None:
    before, after = HashRing(nodes(4)), HashRing(nodes(4))
    after.add_node("node-4")
    moved = [k for k in KEYS if before.get_node(k) != after.get_node(k)]
    assert moved and all(after.get_node(k) == "node-4" for k in moved)


def test_ring_remove_moves_only_the_removed_nodes_keys() -> None:
    before, after = HashRing(nodes(5)), HashRing(nodes(5))
    after.remove_node("node-2")
    moved = [k for k in KEYS if before.get_node(k) != after.get_node(k)]
    assert all(before.get_node(k) == "node-2" for k in moved)
    assert len(moved) / len(KEYS) == pytest.approx(1 / 5, rel=0.25)


def test_modulo_moves_most_keys() -> None:
    before, after = ModuloHasher(nodes(4)), ModuloHasher(nodes(5))
    assert remap_fraction(before, after, KEYS) > 0.7


def test_rendezvous_add_moves_about_one_over_n() -> None:
    before, after = RendezvousHasher(nodes(4)), RendezvousHasher(nodes(5))
    assert remap_fraction(before, after, KEYS) == pytest.approx(1 / 5, rel=0.15)


def test_more_vnodes_balance_load_better() -> None:
    def spread(vnodes: int) -> float:
        ring = HashRing(nodes(10), vnodes=vnodes)
        counts = {n: 0 for n in ring.nodes}
        for key in KEYS:
            counts[ring.get_node(key)] += 1
        return statistics.pstdev(counts.values()) / statistics.mean(counts.values())

    assert spread(200) < spread(1) / 3
