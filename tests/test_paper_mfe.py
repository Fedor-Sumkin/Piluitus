from strategy.paper_mfe import build_paper_mfe_reconciliation


def _trade(**overrides):
    trade = {
        "entry_price": 100.0, "original_volume": 100, "side": "BUY",
        "max_favorable_bbo_excursion_pct": 1.0,
        "entry_commission_rub": 0.0, "realized_entry_commission_rub": 3.54,
        "commission_pct_per_side": 0.0354, "paper_slippage_bps": 1.0,
    }
    trade.update(overrides)
    return trade


def test_paper_mfe_reconciles_gross_net_costs_and_actual_result():
    result = build_paper_mfe_reconciliation(_trade(), 40.0)
    assert result["mfe_reconciliation_status"] == "ESTIMATED_NET_MFE_AVAILABLE"
    assert result["max_favorable_bbo_mfe_gross_rub"] == 100.0
    assert result["mfe_estimated_entry_commission_rub"] == 3.54
    assert result["mfe_estimated_exit_commission_rub"] == 3.5754
    assert result["mfe_estimated_exit_slippage_rub"] == 1.01
    assert result["realized_net_pnl_rub_for_mfe"] == 40.0
    assert result["realized_capture_of_net_mfe_pct"] is not None


def test_paper_mfe_uses_original_quantity_after_partial_exit():
    result = build_paper_mfe_reconciliation(
        _trade(volume=50, original_volume=100, entry_commission_rub=1.77,
               realized_entry_commission_rub=1.77), 10.0,
    )
    assert result["max_favorable_bbo_mfe_gross_rub"] == 100.0
    assert result["mfe_reconciliation_status"] == "ESTIMATED_NET_MFE_AVAILABLE"


def test_paper_mfe_sell_uses_directional_favorable_price():
    result = build_paper_mfe_reconciliation(_trade(side="SELL"), 20.0)
    assert result["max_favorable_bbo_mfe_gross_rub"] == 100.0
    assert result["mfe_estimated_exit_commission_rub"] < 3.54


def test_paper_mfe_missing_cost_basis_is_not_reported_as_zero_cost():
    result = build_paper_mfe_reconciliation(_trade(paper_slippage_bps=None), 2.0)
    assert result["mfe_reconciliation_status"] == "MISSING_REQUIRED_MFE_OR_ENTRY_DATA"
    assert result["max_favorable_bbo_mfe_net_estimate_rub"] is None


def test_closed_trade_fee_is_not_double_counted_after_partial_exit():
    result = build_paper_mfe_reconciliation(
        _trade(status="CLOSED", entry_commission_rub=3.54,
               realized_entry_commission_rub=1.77), 20.0,
    )
    assert result["mfe_estimated_entry_commission_rub"] == 3.54
