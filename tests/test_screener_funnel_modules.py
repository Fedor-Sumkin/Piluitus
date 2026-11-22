"""Structural contracts for the modular screener funnel."""
from __future__ import annotations

import ast
from pathlib import Path

from strategy.screener_funnel import HFTMemoryScreener
from strategy.screener_funnel_order_book import ScreenerFunnelOrderBookMixin
from strategy.screener_funnel_selection import ScreenerFunnelSelectionMixin
from strategy.screener_funnel_state import ScreenerFunnelStateMixin
from strategy.screener_funnel_tick import ScreenerFunnelTickMixin
from strategy.screener_funnel_trade_flow import ScreenerFunnelTradeFlowMixin


ROOT = Path(__file__).resolve().parents[1]


def test_hft_screener_assembles_all_runtime_mixins_over_one_shared_state():
    for mixin in (
        ScreenerFunnelSelectionMixin,
        ScreenerFunnelOrderBookMixin,
        ScreenerFunnelTickMixin,
        ScreenerFunnelTradeFlowMixin,
        ScreenerFunnelStateMixin,
    ):
        assert issubclass(HFTMemoryScreener, mixin)

    expected = {
        ScreenerFunnelStateMixin: ("normalize_ticker", "register_ticker", "set_universe"),
        ScreenerFunnelSelectionMixin: ("_percentile_ranks", "_features_for", "_refresh_selection"),
        ScreenerFunnelOrderBookMixin: (
            "_top_size_drop_pct", "_observe_top_size", "_current_top_size",
            "_current_size_drop", "_sync_push_order_book", "push_order_book",
        ),
        ScreenerFunnelTickMixin: ("_sync_push_tick", "push_tick"),
        ScreenerFunnelTradeFlowMixin: (
            "enable_trade_data_mode", "reset_trade_history", "reset_trade_subscription",
            "push_trade", "refresh_trade_metrics", "_trade_metrics_locked",
        ),
    }
    for owner, methods in expected.items():
        for name in methods:
            method = getattr(HFTMemoryScreener, name)
            assert method.__qualname__.startswith(owner.__name__ + ".")


def test_funnel_primitives_remain_available_from_legacy_module_namespace():
    from strategy import screener_funnel as legacy
    from strategy import screener_funnel_primitives as primitives

    for name in (
        "_env_int", "_env_float", "_event_timestamp_to_monotonic",
        "_best_quote_ofi_delta", "_ofi_window", "_queue_imbalance_window",
        "logger",
    ):
        assert getattr(legacy, name) is getattr(primitives, name)


def test_funnel_mixins_do_not_import_the_facade_or_each_other():
    paths = (
        "screener_funnel_state.py",
        "screener_funnel_selection.py",
        "screener_funnel_order_book.py",
        "screener_funnel_tick.py",
        "screener_funnel_trade_flow.py",
    )
    for filename in paths:
        tree = ast.parse(
            (ROOT / "strategy" / filename).read_text(encoding="utf-8")
        )
        imported = {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }
        assert "strategy.screener_funnel" not in imported
        assert "screener_funnel" not in imported
