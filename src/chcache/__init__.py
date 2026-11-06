"""Distributed cache cluster with consistent hashing."""

from .cluster import ClusterClient, ClusterStats, should_refresh_early
from .hashing import HashRing, ModuloHasher, RendezvousHasher, remap_fraction, stable_hash
from .hotkeys import CountMinSketch, HotKeyDetector
from .network import NodeUnavailable, SimulatedNetwork
from .node import CacheNode, Entry
from .singleflight import SingleFlight


def build_cluster(
    node_count: int = 3,
    capacity_bytes: int = 1 << 20,
    policy: str = "lru",
    vnodes: int = 160,
    **client_kwargs,
) -> ClusterClient:
    """Create a simulated cluster with `node_count` nodes behind one client."""
    network = SimulatedNetwork()
    clock = client_kwargs.get("clock")
    ring = HashRing(vnodes=vnodes)
    client = ClusterClient(network, ring, **client_kwargs)
    for i in range(node_count):
        kwargs = {"clock": clock} if clock else {}
        client.add_node(CacheNode(f"node-{i}", capacity_bytes, policy, **kwargs))
    return client


__all__ = [
    "CacheNode",
    "ClusterClient",
    "ClusterStats",
    "CountMinSketch",
    "Entry",
    "HashRing",
    "HotKeyDetector",
    "ModuloHasher",
    "NodeUnavailable",
    "RendezvousHasher",
    "SimulatedNetwork",
    "SingleFlight",
    "build_cluster",
    "remap_fraction",
    "should_refresh_early",
    "stable_hash",
]
