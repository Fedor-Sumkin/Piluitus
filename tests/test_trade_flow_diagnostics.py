"""Tests for thread-safe TRADE FLOW diagnostic counters."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from monitoring.trade_flow_diagnostics import TradeFlowCounters


def test_trade_flow_counter_accumulates_and_drains_at_sixty_seconds() -> None:
    counters = TradeFlowCounters(started_at=10.0)
    counters.increment("quotes_received")
    counters.increment("quotes_received", 2)
    counters.increment("llm_calls")

    assert counters.snapshot_if_due(now=69.999, interval_sec=60.0) is None
    assert counters.snapshot_if_due(now=70.0, interval_sec=60.0) == {
        "quotes_received": 3,
        "llm_calls": 1,
    }
    assert counters.snapshot_if_due(now=130.0, interval_sec=60.0) == {}


def test_trade_flow_counter_force_snapshot_drains_early() -> None:
    counters = TradeFlowCounters(started_at=100.0)
    counters.increment("orders_opened_buy", 2)

    assert counters.snapshot_if_due(now=101.0, force=True) == {
        "orders_opened_buy": 2,
    }
    assert counters.snapshot_if_due(now=160.999) is None
    assert counters.snapshot_if_due(now=161.0) == {}


def test_trade_flow_counter_reset_clears_counts_and_restarts_window() -> None:
    counters = TradeFlowCounters(started_at=0.0)
    counters.increment("risk_blocked")
    counters.reset(now=500.0)
    counters.increment("llm_calls")

    assert counters.snapshot_if_due(now=559.999) is None
    assert counters.snapshot_if_due(now=560.0) == {"llm_calls": 1}


def test_trade_flow_counter_is_thread_safe() -> None:
    counters = TradeFlowCounters(started_at=0.0)

    def increment_many(_: int) -> None:
        for _ in range(500):
            counters.increment("quotes_received")

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(increment_many, range(8)))

    assert counters.snapshot_if_due(now=60.0) == {
        "quotes_received": 4000,
    }
