from __future__ import annotations

from unittest.mock import patch

from config import ASSET_METADATA
from execution.execution_position_risk import ExecutionPositionRiskMixin


class _Risk(ExecutionPositionRiskMixin):
    def __init__(self):
        self.screener = None


def test_missing_atr_history_has_a_specific_diagnostic():
    risk = _Risk()
    with patch.dict(ASSET_METADATA, {
        "PILUITUS_TEST_ATR": {"price_tick": 0.01, "decimals": 2},
    }):
        sl, tp, reason = risk.calculate_atr_stop_with_diagnostics(
            "PILUITUS_TEST_ATR", 100.0, "BUY", atr_value=0.0, atr_periods=0
        )
    assert (sl, tp) == (0.0, 0.0)
    assert reason == "ATR_HISTORY_UNAVAILABLE"


def test_missing_finam_tick_metadata_is_not_reported_as_missing_atr():
    risk = _Risk()
    with patch.dict(ASSET_METADATA, {}, clear=False):
        ASSET_METADATA.pop("PILUITUS_TEST_ATR", None)
        sl, tp, reason = risk.calculate_atr_stop_with_diagnostics(
            "PILUITUS_TEST_ATR", 100.0, "BUY", atr_value=2.0, atr_periods=5
        )
    assert (sl, tp) == (0.0, 0.0)
    assert reason == "PRICE_TICK_METADATA_UNAVAILABLE"


def test_fixed_finam_tick_builds_valid_directional_buy_and_sell_stops():
    risk = _Risk()
    metadata = {"price_tick": 0.05, "min_step": 5, "decimals": 2}
    with patch.dict(ASSET_METADATA, {"PILUITUS_TEST_ATR": metadata}):
        buy_sl, buy_tp, buy_reason = risk.calculate_atr_stop_with_diagnostics(
            "PILUITUS_TEST_ATR", 100.0, "BUY", atr_value=2.0, atr_periods=5
        )
        sell_sl, sell_tp, sell_reason = risk.calculate_atr_stop_with_diagnostics(
            "PILUITUS_TEST_ATR", 100.0, "SELL", atr_value=2.0, atr_periods=5
        )
        legacy_result = risk.calculate_atr_stop(
            "PILUITUS_TEST_ATR", 100.0, "BUY", atr_value=2.0, atr_periods=5
        )
    assert buy_reason == sell_reason == "OK"
    assert buy_sl < 100.0 < buy_tp
    assert sell_tp < 100.0 < sell_sl
    assert legacy_result == (buy_sl, buy_tp)
    for value in (buy_sl, buy_tp, sell_sl, sell_tp):
        assert round(value / 0.05) == value / 0.05


def test_tiered_finam_price_step_takes_precedence_over_fixed_step():
    risk = _Risk()
    metadata = {
        "price_tick": 0.01,
        "decimals": 2,
        "price_step": {
            "threshold_price": 100.0,
            "step": 0.01,
            "step_from_threshold": 0.05,
        },
    }
    with patch.dict(ASSET_METADATA, {"PILUITUS_TEST_ATR": metadata}):
        tick, origin = risk._price_tick_rule("PILUITUS_TEST_ATR", 100.5)
        rounded = risk._round_price_to_valid_tick(
            "PILUITUS_TEST_ATR", 101.13, __import__("decimal").ROUND_CEILING
        )
    assert (tick, origin) == (0.05, 100.0)
    assert rounded == 101.15
