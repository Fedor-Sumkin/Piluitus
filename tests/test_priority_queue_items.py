from __future__ import annotations

import asyncio

from runtime.priority_queue import PriorityQueueItemFactory


def test_priority_queue_items_have_unique_tie_breakers() -> None:
    async def build_items():
        factory = PriorityQueueItemFactory()
        first_payload = {"ticker": "SBER"}
        second_payload = {"ticker": "GAZP"}
        first = factory.create(1, "MARKET_TICK", first_payload)
        second = factory.create(1, "MARKET_TICK", second_payload)
        return first, second, first_payload, second_payload

    first, second, first_payload, second_payload = asyncio.run(build_items())

    assert first[:2] < second[:2]
    assert first[0] == second[0] == 1
    assert first[1] != second[1]
    assert first[3] == second[3] == "MARKET_TICK"
    assert first[4] is first_payload
    assert second[4] is second_payload


def test_priority_queue_items_preserve_priority_before_sequence() -> None:
    async def build_items():
        factory = PriorityQueueItemFactory()
        lower_priority = factory.create(5, "LOW", {"payload": "a"})
        higher_priority = factory.create(0, "HIGH", {"payload": "b"})
        return lower_priority, higher_priority

    lower_priority, higher_priority = asyncio.run(build_items())

    assert higher_priority < lower_priority
