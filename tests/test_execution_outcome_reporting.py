from execution.outcome_reporting import normalize_execution_fill


LOT_SIZES = {"SBER": 10, "VTBR": 100}


def test_normalize_execution_fill_preserves_executor_values():
    result = normalize_execution_fill(
        {"orderId": "broker-1", "lots": "3", "shares": "30", "notional_entry_rub": "912.5", "requested_lots": "4"},
        requested_lots=5, ticker="SBER", price=31.0, lot_sizes=LOT_SIZES,
    )
    assert result == {
        "order_id": "broker-1",
        "actual_lots": 3,
        "actual_shares": 30,
        "actual_notional": 912.5,
        "requested_lots": 4,
    }


def test_normalize_execution_fill_supports_snake_case_order_id():
    result = normalize_execution_fill(
        {"order_id": "paper-2", "lots": 2, "shares": 20, "notional_entry_rub": 620},
        requested_lots=7, ticker="SBER", price=31.0, lot_sizes=LOT_SIZES,
    )
    assert result["order_id"] == "paper-2"
    assert result["actual_lots"] == 2
    assert result["actual_shares"] == 20
    assert result["actual_notional"] == 620


def test_normalize_execution_fill_falls_back_to_requested_lots():
    result = normalize_execution_fill(
        {}, requested_lots=3, ticker="SBER", price=31.25, lot_sizes=LOT_SIZES,
    )
    assert result["actual_lots"] == 3
    assert result["actual_shares"] == 30
    assert result["actual_notional"] == 937.5
    assert result["requested_lots"] is None


def test_normalize_execution_fill_uses_zero_lot_multiplier_when_unknown():
    result = normalize_execution_fill(
        {"lots": 2}, requested_lots=5, ticker="UNKNOWN", price=10.0, lot_sizes=LOT_SIZES,
    )
    assert result["actual_lots"] == 2
    assert result["actual_shares"] == 0
    assert result["actual_notional"] == 0.0


def test_normalize_execution_fill_does_not_invent_shares_for_invalid_lots():
    result = normalize_execution_fill(
        {"lots": "invalid"}, requested_lots=4, ticker="SBER", price=10.0, lot_sizes=LOT_SIZES,
    )
    assert result["actual_lots"] is None
    assert result["actual_shares"] is None
    assert result["actual_notional"] is None


def test_normalize_execution_fill_uses_executor_requested_lots_when_valid():
    result = normalize_execution_fill(
        {"requested_lots": "8"}, requested_lots=4, ticker="SBER", price=10.0, lot_sizes=LOT_SIZES,
    )
    assert result["requested_lots"] == 8
    assert result["actual_lots"] == 4
    assert result["actual_shares"] == 40
