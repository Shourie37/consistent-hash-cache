### Hit rate vs cache size

Zipf s=0.99, 10,000 keys, 200,000 requests, 106-byte items, cold start.

| Cache size (% of keys) | Bytes | LRU 1 node | LFU 1 node | LRU 4-node ring | LFU 4-node ring |
|---|---|---|---|---|---|
| 1% | 10 KiB | 37.6% | 47.9% | 37.5% | 47.8% |
| 2% | 20 KiB | 45.9% | 55.0% | 45.9% | 55.1% |
| 5% | 51 KiB | 57.1% | 64.8% | 57.1% | 65.0% |
| 10% | 103 KiB | 66.1% | 71.9% | 66.0% | 72.0% |
| 20% | 207 KiB | 75.5% | 79.4% | 75.5% | 79.4% |
| 50% | 517 KiB | 88.3% | 89.3% | 88.3% | 89.2% |

### Keys remapped when the cluster scales

100,000 keys. Share of keys whose primary node changes.

| Change | Ideal | Ring (160 vnodes) | Rendezvous | Modulo |
|---|---|---|---|---|
| 4 -> 5 (add 1) | 20.0% | 20.0% | 19.9% | 80.1% |
| 10 -> 11 (add 1) | 9.1% | 9.1% | 9.0% | 90.9% |
| 10 -> 9 (remove 1) | 10.0% | 9.1% | 10.1% | 90.0% |
| 50 -> 51 (add 1) | 2.0% | 2.0% | 2.0% | 98.0% |

### Load balance vs virtual nodes

10 nodes, 100,000 keys (mean 10,000 per node).

| Virtual nodes per node | Std dev (keys) | Std dev / mean | Max / mean | Min / mean |
|---|---|---|---|---|
| 1 | 9,967 | 99.7% | 3.68x | 0.20x |
| 4 | 5,101 | 51.0% | 2.17x | 0.44x |
| 16 | 3,065 | 30.6% | 1.73x | 0.61x |
| 64 | 1,128 | 11.3% | 1.19x | 0.84x |
| 160 | 877 | 8.8% | 1.18x | 0.90x |
| 512 | 353 | 3.5% | 1.07x | 0.93x |
| rendezvous | 125 | 1.3% | 1.02x | 0.98x |

### Stampede protection

Simulated: one hot key, 2,000 req/s for 60 s, TTL 5 s, loader takes 100 ms, warm start.

| Strategy | Backend loads | Peak concurrent loads | Requests that waited on a load |
|---|---|---|---|
| naive | 2,189 | 216 | 2,189 |
| single-flight | 11 | 1 | 2,239 |
| single-flight + early expiration | 13 | 1 | 13 |

Measured with real threads and the real client:

| 64 threads miss one key at once | Backend loads |
|---|---|
| no coalescing | 64 |
| single-flight (`get_or_load`) | 1 |
