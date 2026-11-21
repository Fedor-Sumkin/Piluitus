from __future__ import annotations

from portfolio.dashboard_accounting import build_dashboard_balance_snapshot


def test_snapshot_uses_acquisition_cost_not_current_market_price():
    result = build_dashboard_balance_snapshot(
        positions=[{"id": "p1", "ticker": "SBER", "status": "OPEN", "volume": 10,
                    "entry_price": 250.0, "current_price": 400.0}],
        initial_deposit=100_000, total_pnl=1_500, realized_pnl=500, open_pnl=1_000,
        lot_sizes={"SBER": 1},
    )
    snapshot = result["snapshot"]
    assert result["assets_used"] == 2_500.0
    assert snapshot["margin_used"] == 2_500.0
    assert snapshot["free_margin"] == 98_000.0
    assert snapshot["equity"] == 101_500.0
    assert snapshot["open_pnl"] == 1_000.0


def test_unresolved_positions_reserve_capital_without_double_counting_ids():
    active = [{"id": "same", "ticker": "SBER", "status": "OPEN", "volume": 10, "entry_price": 100.0}]
    unresolved = [
        {"id": "same", "ticker": "SBER", "status": "STALE_REVIEW", "volume": 10,
         "entry_price": 100.0, "reconciliation_status": "REVIEW_REQUIRED"},
        {"id": "old", "ticker": "GAZP", "status": "STALE_REVIEW", "volume": 5,
         "entry_price": 200.0, "reconciliation_status": "REVIEW_REQUIRED"},
    ]
    result = build_dashboard_balance_snapshot(
        positions=active, unresolved_positions=unresolved, initial_deposit=100_000,
        lot_sizes={"SBER": 1, "GAZP": 1},
    )
    assert result["assets_used"] == 2_000.0
    assert result["unresolved_assets_cost"] == 2_000.0
    assert result["snapshot"]["unreconciled_positions_count"] == 2
    assert result["snapshot"]["active_positions_count"] == 1


def test_snapshot_caps_exposure_percentages_and_reports_over_cap():
    result = build_dashboard_balance_snapshot(
        positions=[{"id": "p", "ticker": "SBER", "volume": 20_000, "entry_price": 100}],
        initial_deposit=100_000, max_gross_exposure_pct=0.90, max_single_position_pct=-1,
        lot_sizes={"SBER": 1},
    )
    assert result["max_gross_pct"] == 0.15
    assert result["snapshot"]["single_position_cap"] == 0.0
    assert result["exposure_cap"] == 15_000.0
    assert result["over_cap"] is True
    assert result["guard_signature"] == (0, True)


def test_snapshot_sanitizes_non_finite_pnl_and_preserves_negative_free_balance():
    result = build_dashboard_balance_snapshot(
        positions=[{"id": "p", "ticker": "SBER", "volume": 20, "entry_price": 100}],
        initial_deposit=1_000, total_pnl=float("nan"), realized_pnl=float("inf"),
        open_pnl=float("-inf"), lot_sizes={"SBER": 1},
    )
    assert result["snapshot"]["pnl"] == 0.0
    assert result["snapshot"]["realized_pnl"] == 0.0
    assert result["snapshot"]["open_pnl"] == 0.0
    assert result["snapshot"]["free_margin"] == -1_000.0
