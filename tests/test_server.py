import asyncio

from chcache.server import run_demo


def test_three_node_tcp_demo_survives_one_stop() -> None:
    result = asyncio.run(run_demo(keys=30))
    assert sum(result["keys_per_node"].values()) == 60
    assert result["readable_after_stop"] == 30
