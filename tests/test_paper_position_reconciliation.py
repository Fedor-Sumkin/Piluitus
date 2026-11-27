from strategy.paper_position_reconciliation import audit_open_paper_position


def _trade(**updates):
    trade = {
        "id": "PAPER-1",
        "ticker": "SBER",
        "side": "BUY",
        "status": "OPEN",
        "entry_price": 100.0,
        "initial_stop_loss": 99.0,
        "stop_loss": 100.5,
        "take_profit": 103.0,
        "original_volume": 100,
        "volume": 50,
        "original_lots": 2,
        "lots": 1,
        "lot_size": 50,
        "exit_partial_fills": [{
            "quantity_shares": 50,
            "fill_price_after_slippage": 101.0,
            "net_pnl_rub": 50.0,
        }],
        "exit_partial_fill_count": 1,
        "realized_net_pnl_rub": 50.0,
    }
    trade.update(updates)
    return trade


def test_restored_position_passes_complete_partial_fill_reconciliation():
    result = audit_open_paper_position(_trade(), expected_order_id="PAPER-1")
    assert result["valid"] is True
    assert result["status"] == "VERIFIED"
    assert result["remaining_shares"] == 50
    assert result["original_shares"] == 100
    assert result["partial_fills_verified"] is True


def test_partial_fill_quantity_mismatch_requires_manual_review():
    trade = _trade(exit_partial_fills=[{
        "quantity_shares": 25,
        "fill_price_after_slippage": 101.0,
        "net_pnl_rub": 25.0,
    }])
    result = audit_open_paper_position(trade)
    assert result["valid"] is False
    assert "PARTIAL_FILL_QUANTITY_DOES_NOT_RECONCILE" in result["reasons"]


def test_partial_fill_pnl_mismatch_requires_manual_review():
    trade = _trade(realized_net_pnl_rub=70.0)
    result = audit_open_paper_position(trade)
    assert result["valid"] is False
    assert "PARTIAL_FILL_NET_PNL_DOES_NOT_RECONCILE" in result["reasons"]


def test_wrong_journal_key_is_not_restored_under_another_order_id():
    result = audit_open_paper_position(_trade(), expected_order_id="PAPER-OTHER")
    assert result["valid"] is False
    assert "ORDER_ID_KEY_MISMATCH" in result["reasons"]

def test_truncated_partial_fill_ledger_requires_manual_review():
    fills = [{
        "quantity_shares": 1,
        "fill_price_after_slippage": 101.0,
        "net_pnl_rub": 1.0,
    }] * 100
    trade = _trade(
        original_volume=200, volume=100, original_lots=4, lots=2,
        exit_partial_fills=fills, exit_partial_fill_count=101,
        realized_net_pnl_rub=100.0,
    )
    result = audit_open_paper_position(trade)
    assert result["valid"] is False
    assert "PARTIAL_FILL_LEDGER_TRUNCATED_REQUIRES_REVIEW" in result["reasons"]


def test_reduced_open_position_without_partial_ledger_requires_review():
    trade = _trade(exit_partial_fills=None, exit_partial_fill_count=0)
    result = audit_open_paper_position(trade)
    assert result["valid"] is False
    assert "PARTIAL_FILL_LEDGER_MISSING_FOR_REDUCED_POSITION" in result["reasons"]
