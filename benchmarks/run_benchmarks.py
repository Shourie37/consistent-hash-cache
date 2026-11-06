"""Run every benchmark and print Markdown tables. Seeded, so output is repeatable."""

from __future__ import annotations

import bisect
import itertools
import random
import statistics
import threading
import time
from pathlib import Path

from chcache import (
    CacheNode,
    HashRing,
    ModuloHasher,
    RendezvousHasher,
    build_cluster,
    remap_fraction,
    should_refresh_early,
)
from chcache.node import Entry, sizeof

SEED = 42


def table(headers: list[str], rows: list[list[object]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(lines)


def zipf_sampler(n: int, s: float, rng: random.Random):
    weights = [1 / (rank**s) for rank in range(1, n + 1)]
    cdf = list(itertools.accumulate(weights))
    total = cdf[-1]
    return lambda: bisect.bisect_left(cdf, rng.random() * total)


# 1. Hit rate vs cache size


def hit_rate_single(policy: str, capacity: int, trace: list[str], value: bytes) -> float:
    node = CacheNode("n", capacity, policy)
    hits = 0
    for key in trace:
        if node.get(key) is not None:
            hits += 1
        else:
            node.set(key, value, 1)
    return hits / len(trace)


def hit_rate_cluster(policy: str, capacity: int, trace: list[str], value: bytes) -> float:
    client = build_cluster(
        4, capacity // 4, policy, replication=1, read_repair=False, hot_threshold=10**9
    )
    hits = 0
    for key in trace:
        if client.get(key) is not None:
            hits += 1
        else:
            client.set(key, value)
    return hits / len(trace)


def bench_hit_rate() -> str:
    keys, requests, value = 10_000, 200_000, b"x" * 100
    rng = random.Random(SEED)
    sample = zipf_sampler(keys, 0.99, rng)
    trace = [f"k{sample():05d}" for _ in range(requests)]
    item = sizeof("k00000", value)
    rows = []
    for pct in [1, 2, 5, 10, 20, 50]:
        capacity = keys * item * pct // 100
        rows.append(
            [
                f"{pct}%",
                f"{capacity // 1024} KiB",
                f"{hit_rate_single('lru', capacity, trace, value):.1%}",
                f"{hit_rate_single('lfu', capacity, trace, value):.1%}",
                f"{hit_rate_cluster('lru', capacity, trace, value):.1%}",
                f"{hit_rate_cluster('lfu', capacity, trace, value):.1%}",
            ]
        )
    head = ["Cache size (% of keys)", "Bytes", "LRU 1 node", "LFU 1 node", "LRU 4-node ring", "LFU 4-node ring"]
    note = f"Zipf s=0.99, {keys:,} keys, {requests:,} requests, {item}-byte items, cold start."
    return note + "\n\n" + table(head, rows)


# 2. Keys remapped on scaling


def bench_remap() -> str:
    keys = [f"key-{i}" for i in range(100_000)]
    rows = []
    for start, end in [(4, 5), (10, 11), (10, 9), (50, 51)]:
        names = lambda n: [f"node-{i}" for i in range(n)]  # noqa: E731
        moved = {}
        for label, cls in [("ring", HashRing), ("modulo", ModuloHasher), ("rendezvous", RendezvousHasher)]:
            moved[label] = remap_fraction(cls(names(start)), cls(names(end)), keys)
        ideal = abs(end - start) / max(start, end)
        action = "add 1" if end > start else "remove 1"
        rows.append(
            [
                f"{start} -> {end} ({action})",
                f"{ideal:.1%}",
                f"{moved['ring']:.1%}",
                f"{moved['rendezvous']:.1%}",
                f"{moved['modulo']:.1%}",
            ]
        )
    head = ["Change", "Ideal", "Ring (160 vnodes)", "Rendezvous", "Modulo"]
    return "100,000 keys. Share of keys whose primary node changes.\n\n" + table(head, rows)


# 3. Load balance vs virtual nodes


def spread(placement, keys: list[str]) -> list[str]:
    counts = {n: 0 for n in placement.nodes}
    for key in keys:
        counts[placement.get_node(key)] += 1
    values = list(counts.values())
    mean = statistics.mean(values)
    sd = statistics.pstdev(values)
    return [f"{sd:,.0f}", f"{sd / mean:.1%}", f"{max(values) / mean:.2f}x", f"{min(values) / mean:.2f}x"]


def bench_balance() -> str:
    keys = [f"key-{i}" for i in range(100_000)]
    nodes = [f"node-{i}" for i in range(10)]
    rows = [[str(v), *spread(HashRing(nodes, vnodes=v), keys)] for v in [1, 4, 16, 64, 160, 512]]
    rows.append(["rendezvous", *spread(RendezvousHasher(nodes), keys)])
    head = ["Virtual nodes per node", "Std dev (keys)", "Std dev / mean", "Max / mean", "Min / mean"]
    return "10 nodes, 100,000 keys (mean 10,000 per node).\n\n" + table(head, rows)


# 4. Stampede protection


def simulate_stampede(strategy: str, seed: int) -> dict[str, int]:
    """Discrete-event model of one hot key under Poisson traffic."""
    rate, duration, ttl, latency = 2000.0, 60.0, 5.0, 0.1
    rng = random.Random(seed)
    xfetch_rng = random.Random(seed + 1)
    entry = Entry("v", 1, ttl, latency)  # warm start
    inflight: list[float] = []
    loads = blocked = peak = 0
    t = 0.0
    while t < duration:
        t += rng.expovariate(rate)
        while inflight and inflight[0] <= t:
            done = inflight.pop(0)
            entry = Entry("v", 1, done + ttl, latency)
        fresh = entry is not None and t < entry.expires_at
        if fresh:
            if (
                strategy == "single-flight + early expiration"
                and not inflight
                and should_refresh_early(entry, t, 1.0, xfetch_rng)
            ):
                loads += 1
                blocked += 1
                inflight.append(t + latency)
            continue
        blocked += 1
        if strategy == "naive" or not inflight:
            loads += 1
            inflight.append(t + latency)
            inflight.sort()
        peak = max(peak, len(inflight))
    return {"loads": loads, "blocked": blocked, "peak": max(peak, 1 if loads else 0)}


def threaded_stampede(single_flight: bool, callers: int = 64) -> int:
    """Real threads against a real cluster. Return backend load count."""
    client = build_cluster(3, 1 << 20, replication=2)
    calls = []
    lock = threading.Lock()
    barrier = threading.Barrier(callers)

    def loader() -> str:
        with lock:
            calls.append(1)
        time.sleep(0.05)
        return "v"

    def worker() -> None:
        barrier.wait()
        if single_flight:
            client.get_or_load("hot", loader, ttl=60)
        elif client.get("hot") is None:
            client.set("hot", loader(), ttl=60)

    threads = [threading.Thread(target=worker) for _ in range(callers)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    return len(calls)


def bench_stampede() -> str:
    rows = []
    for strategy in ["naive", "single-flight", "single-flight + early expiration"]:
        r = simulate_stampede(strategy, SEED)
        rows.append([strategy, f"{r['loads']:,}", r["peak"], f"{r['blocked']:,}"])
    head = ["Strategy", "Backend loads", "Peak concurrent loads", "Requests that waited on a load"]
    sim = (
        "Simulated: one hot key, 2,000 req/s for 60 s, TTL 5 s, loader takes 100 ms, warm start.\n\n"
        + table(head, rows)
    )
    naive, coalesced = threaded_stampede(False), threaded_stampede(True)
    real = table(
        ["64 threads miss one key at once", "Backend loads"],
        [["no coalescing", naive], ["single-flight (`get_or_load`)", coalesced]],
    )
    return sim + "\n\nMeasured with real threads and the real client:\n\n" + real


def main() -> None:
    sections = [
        ("Hit rate vs cache size", bench_hit_rate),
        ("Keys remapped when the cluster scales", bench_remap),
        ("Load balance vs virtual nodes", bench_balance),
        ("Stampede protection", bench_stampede),
    ]
    out = []
    for title, fn in sections:
        start = time.perf_counter()
        body = fn()
        out.append(f"### {title}\n\n{body}\n")
        print(f"{title}: {time.perf_counter() - start:.1f}s", flush=True)
    text = "\n".join(out)
    Path(__file__).with_name("results.md").write_text(text)
    print()
    print(text)


if __name__ == "__main__":
    main()
