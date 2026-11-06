import threading
import time

import pytest

from chcache import CacheNode, ClusterClient, HashRing, SimulatedNetwork, build_cluster


def cluster(clock, nodes: int = 5, **kwargs) -> ClusterClient:
    return build_cluster(nodes, 1 << 20, clock=clock, replication=3, **kwargs)


def holders(client: ClusterClient, key: str) -> list[str]:
    return [n for n, node in client.network.nodes.items() if node.peek(key) is not None]


def test_write_goes_to_n_replicas(clock) -> None:
    client = cluster(clock)
    assert client.set("k", "v") == 3
    assert sorted(holders(client, "k")) == sorted(client.owners("k"))
    assert client.get("k") == "v"


def test_read_survives_replica_failures(clock) -> None:
    client = cluster(clock)
    client.set("k", "v")
    owners = client.owners("k")
    client.network.fail(owners[0])
    client.network.fail(owners[1])
    assert client.get("k") == "v"


def test_read_repair_fixes_stale_replica(clock) -> None:
    client = cluster(clock)
    client.set("k", "old")
    stale = client.owners("k")[2]
    client.network.fail(stale)
    client.set("k", "new")
    client.network.recover(stale)
    client.hints.clear()
    assert client.network.nodes[stale].peek("k").value == "old"
    assert client.get("k") == "new"
    assert client.network.nodes[stale].peek("k").value == "new"
    assert client.stats.read_repairs >= 1


def test_read_repair_fills_missing_replica(clock) -> None:
    client = cluster(clock)
    client.set("k", "v")
    owner = client.owners("k")[1]
    client.network.nodes[owner].delete("k")
    client.get("k")
    assert client.network.nodes[owner].peek("k").value == "v"


def test_read_repair_can_be_disabled(clock) -> None:
    client = cluster(clock, read_repair=False)
    client.set("k", "v")
    owner = client.owners("k")[1]
    client.network.nodes[owner].delete("k")
    client.get("k")
    assert client.network.nodes[owner].peek("k") is None


def test_hinted_handoff_writes_to_substitute_and_replays(clock) -> None:
    client = cluster(clock)
    owner = client.owners("k")[0]
    client.network.fail(owner)
    assert client.set("k", "v") == 3
    assert client.stats.hinted_writes == 1
    substitute = client.hints[owner]["k"]
    assert substitute not in client.owners("k")
    assert client.network.nodes[substitute].peek("k").value == "v"

    assert client.replay_hints() == 0  # owner still down
    client.network.recover(owner)
    assert client.replay_hints() == 1
    assert client.network.nodes[owner].peek("k").value == "v"
    assert client.network.nodes[substitute].peek("k") is None
    assert not client.hints


def test_hint_replay_does_not_overwrite_newer_value(clock) -> None:
    client = cluster(clock)
    owner = client.owners("k")[0]
    client.network.fail(owner)
    client.set("k", "hinted")
    client.network.recover(owner)
    client.network.nodes[owner].set("k", "newer", version=10**30)
    client.replay_hints()
    assert client.network.nodes[owner].peek("k").value == "newer"


def test_delete_tombstone_blocks_resurrection(clock) -> None:
    client = cluster(clock)
    client.set("k", "v")
    owner = client.owners("k")[0]
    client.network.fail(owner)
    client.delete("k")
    client.network.recover(owner)
    client.hints.clear()
    assert client.get("k") is None
    assert client.get("k") is None


def test_write_fails_when_all_nodes_down(clock) -> None:
    client = cluster(clock, nodes=3)
    for name in list(client.network.nodes):
        client.network.fail(name)
    assert client.set("k", "v") == 0
    assert client.stats.failed_writes == 1


def test_none_value_is_rejected(clock) -> None:
    with pytest.raises(ValueError):
        cluster(clock).set("k", None)


def test_hot_key_served_from_near_cache(clock) -> None:
    client = cluster(clock, hot_threshold=10, near_cache_ttl=1.0)
    client.set("hot", "v")
    for _ in range(20):
        assert client.get("hot") == "v"
    calls = client.network.calls
    for _ in range(50):
        client.get("hot")
    assert client.network.calls == calls
    assert client.stats.near_cache_hits >= 50


def test_near_cache_is_invalidated_on_local_write(clock) -> None:
    client = cluster(clock, hot_threshold=2)
    client.set("hot", "a")
    for _ in range(5):
        client.get("hot")
    client.set("hot", "b")
    assert client.get("hot") == "b"


def test_near_cache_entry_expires(clock) -> None:
    client = cluster(clock, hot_threshold=2, near_cache_ttl=0.5)
    client.set("hot", "a")
    for _ in range(5):
        client.get("hot")
    calls = client.network.calls
    clock.advance(0.6)
    client.get("hot")
    assert client.network.calls > calls


def test_get_or_load_caches_value(clock) -> None:
    client = cluster(clock)
    loads = []
    for _ in range(5):
        assert client.get_or_load("k", lambda: loads.append(1) or "v", ttl=60) == "v"
    assert len(loads) == 1


def test_early_expiration_refreshes_before_ttl(clock) -> None:
    client = cluster(clock, early_expiration_beta=1.0)

    def loader() -> str:
        clock.advance(2.0)  # recompute takes 2 s
        return "v"

    client.get_or_load("k", loader, ttl=10)
    clock.advance(9.5)  # 0.5 s before expiry, delta is 2 s
    for _ in range(20):
        client.get_or_load("k", loader, ttl=10)
        if client.stats.early_refreshes:
            break
    assert client.stats.early_refreshes >= 1


def test_no_early_refresh_far_from_expiry(clock) -> None:
    client = cluster(clock)
    client.set("k", "v", ttl=1000, delta=0.01)
    for _ in range(100):
        client.get_or_load("k", lambda: "x", ttl=1000)
    assert client.stats.early_refreshes == 0


def test_get_or_load_single_flight_under_concurrency() -> None:
    client = build_cluster(3, 1 << 20, replication=2)
    calls = []
    barrier = threading.Barrier(32)

    def loader() -> str:
        calls.append(1)
        time.sleep(0.2)
        return "v"

    results = []

    def worker() -> None:
        barrier.wait()
        results.append(client.get_or_load("k", loader, ttl=60))

    threads = [threading.Thread(target=worker) for _ in range(32)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results == ["v"] * 32
    assert len(calls) == 1


def test_add_node_joins_ring(clock) -> None:
    network = SimulatedNetwork()
    client = ClusterClient(network, HashRing(), replication=2, clock=clock)
    client.add_node(CacheNode("a", 1000, clock=clock))
    client.add_node(CacheNode("b", 1000, clock=clock))
    assert client.set("k", "v") == 2
