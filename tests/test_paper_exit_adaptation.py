from strategy.paper_exit_adaptation import calculate_adaptive_trailing_gap


def test_adaptive_gap_preserves_configured_distance_when_market_is_unchanged():
    result = calculate_adaptive_trailing_gap(
        initial_risk_distance=1.0,
        base_gap_r=0.75,
        entry_atr=0.5,
        current_atr=0.5,
        entry_spread=0.02,
        current_spread=0.02,
        entry_exit_top_size_raw=100,
        current_exit_top_size_raw=100,
    )
    assert result["available"] is True
    assert result["gap_distance"] == 0.75
    assert result["atr_ratio"] == 1.0
    assert result["liquidity_ratio"] == 1.0


def test_volatility_and_spread_expansion_widen_gap_with_a_hard_cap():
    result = calculate_adaptive_trailing_gap(
        initial_risk_distance=1.0,
        base_gap_r=0.75,
        entry_atr=0.5,
        current_atr=2.0,
        entry_spread=0.02,
        current_spread=0.60,
        entry_exit_top_size_raw=100,
        current_exit_top_size_raw=100,
    )
    assert result["atr_ratio"] == 1.5
    assert result["spread_buffer"] == 0.25
    assert result["gap_distance"] <= 1.5


def test_thinning_same_side_top_liquidity_tightens_gap_without_unit_conversion():
    healthy = calculate_adaptive_trailing_gap(
        initial_risk_distance=1.0, base_gap_r=0.75,
        entry_exit_top_size_raw=100, current_exit_top_size_raw=100,
    )
    thin = calculate_adaptive_trailing_gap(
        initial_risk_distance=1.0, base_gap_r=0.75,
        entry_exit_top_size_raw=100, current_exit_top_size_raw=0,
    )
    assert thin["gap_distance"] < healthy["gap_distance"]
    assert thin["liquidity_ratio"] == 0.0
    assert thin["liquidity_basis"] == "RELATIVE_RAW_TOP_SIZE_NO_UNIT_CONVERSION"


def test_invalid_risk_returns_unavailable_without_guessing():
    result = calculate_adaptive_trailing_gap(
        initial_risk_distance=0, base_gap_r=0.75
    )
    assert result["available"] is False
    assert result["reason"] == "INVALID_RISK_OR_GAP"

def test_execution_engine_adapts_initial_stop_when_atr_contracts():
    from execution_layer import BaseExecutionEngine

    engine = BaseExecutionEngine.__new__(BaseExecutionEngine)
    engine.paper_trailing_enabled = True
    engine.paper_trailing_arm_r = 1.5
    engine.paper_trailing_distance_r = 0.75
    trade = {
        "side": "BUY",
        "entry_price": 100.0,
        "stop_loss": 99.0,
        "initial_stop_loss": 99.0,
        "take_profit": 103.0,
        "paper_trailing_high_water_price": 100.0,
        "atr_at_entry": 1.0,
        "entry_bid": 99.9,
        "entry_ask": 100.1,
        "entry_exit_top_size_raw": 100.0,
    }
    updated = engine._update_paper_trailing_stop(
        "SBER", trade, 100.2,
        {
            "entry_atr": 1.0,
            "current_atr": 0.6,
            "entry_spread": 0.2,
            "current_spread": 0.2,
            "entry_exit_top_size_raw": 100.0,
            "current_exit_top_size_raw": 100.0,
        },
    )
    assert updated is not None
    assert 99.0 < updated < 100.0
    assert trade["stop_loss_stage"] == "ADAPTIVE_INITIAL"
    assert trade["paper_adaptive_initial_stop_distance"] < 1.0


def test_execution_engine_adapts_trailing_gap_to_atr_spread_and_liquidity():
    from execution_layer import BaseExecutionEngine

    engine = BaseExecutionEngine.__new__(BaseExecutionEngine)
    engine.paper_trailing_enabled = True
    engine.paper_trailing_arm_r = 1.0
    engine.paper_trailing_distance_r = 0.75
    trade = {
        "side": "BUY",
        "entry_price": 100.0,
        "stop_loss": 99.0,
        "initial_stop_loss": 99.0,
        "take_profit": 104.0,
        "paper_trailing_high_water_price": 102.0,
        "atr_at_entry": 1.0,
        "entry_bid": 99.9,
        "entry_ask": 100.1,
        "entry_exit_top_size_raw": 100.0,
    }
    updated = engine._update_paper_trailing_stop(
        "SBER", trade, 101.8,
        {
            "entry_atr": 1.0,
            "current_atr": 1.5,
            "entry_spread": 0.2,
            "current_spread": 0.4,
            "entry_exit_top_size_raw": 100.0,
            "current_exit_top_size_raw": 50.0,
        },
    )
    assert updated is not None
    assert updated > 100.0
    assert trade["stop_loss_stage"] == "TRAILING"
    adaptation = trade["paper_trailing_adaptation"]
    assert adaptation["atr_ratio"] > 1.0
    assert adaptation["spread_buffer"] > 0.0
    assert adaptation["liquidity_ratio"] == 0.5
