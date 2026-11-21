from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from storage.paper_research_runtime import PaperResearchRuntime


class Tracker:
    def __init__(self):
        self.records = [{"trade_id": "REALIZED-1"}]
        self.scorecard_calls = []

    def scorecard(self, labels):
        self.scorecard_calls.append(labels)
        return {"overall": {"trades": len(self.records)}}


class DB:
    def __init__(self, trades=None, events=None, snapshot=None):
        self.trades = trades or {}
        self.events = list(events or [])
        self.snapshot = snapshot or {"paper_trades": []}
        self.get_list_calls = []
        self.set_json_calls = []
        self.history_rows = []

    async def get_all_daily_trades(self):
        return self.trades

    async def get_list(self, key, *, start, stop):
        self.get_list_calls.append((key, start, stop))
        return list(self.events)

    async def get_paper_session_snapshot(self):
        return self.snapshot

    async def set_json(self, key, value):
        self.set_json_calls.append((key, dict(value)))

    async def push_to_list(self, key, value, *, max_len):
        self.history_rows.append((key, dict(value), max_len))


class Manager:
    def __init__(self):
        self.broadcasts = 0

    async def broadcast(self):
        self.broadcasts += 1


def make_runtime(
    *,
    db=None,
    engine=None,
    tracker=None,
    labels=None,
    lock=None,
    calls=None,
    now=None,
):
    db = db or DB()
    engine = engine or SimpleNamespace(
        paper_mode=True,
        _maintenance_active=False,
        _opening_tickers=set(),
        _lock_closing=False,
        paper_positions={},
        _unreconciled_paper_positions={},
        closed_trades=[],
    )
    tracker = tracker or Tracker()
    labels = labels if labels is not None else [{"candidate_id": "C-1"}]
    lock = lock or asyncio.Lock()
    calls = calls if calls is not None else []
    manager = Manager()

    def record(name, result):
        def callback(*args, **kwargs):
            calls.append((name, args, kwargs))
            return result
        return callback

    def replay_batch(events, positions, **kwargs):
        calls.append(("replay_batch", (events, positions), kwargs))
        return {"trades": [{"position_id": row.get("id")} for row in positions]}

    def replay_one(events, trade, **kwargs):
        calls.append(("replay_one", (events, trade), kwargs))
        return {"position_id": trade.get("id"), "scenarios": []}

    runtime = PaperResearchRuntime(
        db_manager=db,
        is_paper_trade=lambda row, order_id: str(row.get("mode", "")).upper() == "PAPER",
        execution_engine=engine,
        screener=SimpleNamespace(bbo_max_age_sec=1.25),
        paper_performance_tracker=tracker,
        paper_shadow_labels=labels,
        paper_session_lock=lock,
        broadcast_paper_tables=manager.broadcast,
        bbo_replay_storage_key="paper:bbo",
        bbo_replay_storage_limit=5000,
        paper_closed_trades_storage_limit=20000,
        shadow_outcomes_storage_limit=20000,
        lot_sizes={"SBER": 10},
        parse_finite_float=lambda value: (
            None if value is None else float(value)
        ),
        replay_exit_batch=replay_batch,
        replay_exit_scenarios=replay_one,
        build_shadow_reconciliation=record("shadow_reconciliation", {"matched": 1}),
        build_actual_vs_replay_reconciliation=record(
            "actual_reconciliation", {"matched_exact_identity": 1}
        ),
        build_shadow_cost_sensitivity=record("shadow_cost", {"scenarios": []}),
        build_shadow_walk_forward_validation=record("walk_forward", {"folds": []}),
        now=now or (lambda: datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)),
    )
    return runtime, db, engine, tracker, labels, manager, calls


def test_performance_returns_retained_limit_and_scorecard():
    runtime, _, tracker_engine, tracker, labels, _, _ = make_runtime()
    result = runtime.get_performance()
    assert result == {
        "paper_mode": True,
        "retained_trade_limit": 20000,
        "scorecard": {"overall": {"trades": 1}},
    }
    assert tracker.scorecard_calls == [labels]


def test_research_runtime_rejects_calls_outside_paper_mode():
    runtime, _, engine, _, _, _, _ = make_runtime()
    engine.paper_mode = False
    with pytest.raises(HTTPException) as error:
        runtime.get_performance()
    assert error.value.status_code == 403


def test_batch_replay_filters_and_sorts_closed_paper_rows():
    trades = {
        "A": {
            "id": "A", "mode": "PAPER", "status": "CLOSED",
            "ticker": "SBER@TQBR", "bbo_replay_session_id": "S1",
            "exit_timestamp": "2026-11-21T11:00:00",
        },
        "B": {
            "id": "B", "mode": "PAPER", "status": "CLOSED",
            "ticker": "GAZP", "bbo_replay_session_id": "S2",
            "exit_timestamp": "2026-11-21T12:00:00",
        },
        "C": {
            "id": "C", "mode": "PAPER", "status": "OPEN",
            "ticker": "SBER", "bbo_replay_session_id": "S1",
        },
        "D": {
            "id": "D", "mode": "LIVE", "status": "CLOSED",
            "ticker": "SBER", "bbo_replay_session_id": "S1",
        },
        "E": {
            "id": "E", "mode": "PAPER", "status": "CLOSED",
            "ticker": "SBER", "bbo_replay_session_id": "",
        },
    }
    events = [{"session_id": "S1", "ticker": "SBER"}]
    runtime, db, _, _, _, _, calls = make_runtime(
        db=DB(trades=trades, events=events)
    )
    result = asyncio.run(runtime.replay_batch(ticker="sber@tqbr", limit=10))

    assert result["selected_positions"] == 1
    assert result["candidate_positions"] == 1
    assert result["skipped"] == {
        "NOT_CLOSED_PAPER": 2,
        "NO_REPLAY_SESSION": 1,
        "TICKER_FILTERED": 1,
        "SESSION_FILTERED": 0,
    }
    assert result["journal_events_loaded"] == 1
    assert db.get_list_calls == [("paper:bbo", 0, 4999)]
    batch_call = next(row for row in calls if row[0] == "replay_batch")
    assert batch_call[1][1][0]["id"] == "A"
    assert batch_call[2]["max_bbo_age_sec"] == 1.25
    assert result["execution_reconciliation"] == {"matched_exact_identity": 1}


def test_batch_replay_returns_empty_snapshot_when_no_closed_paper_rows():
    runtime, db, _, _, _, _, calls = make_runtime(
        db=DB(trades={
            "OPEN": {
                "id": "OPEN", "mode": "PAPER", "status": "OPEN",
                "ticker": "SBER", "bbo_replay_session_id": "S1",
            }
        })
    )
    result = asyncio.run(runtime.replay_batch())
    assert result["selected_positions"] == 0
    assert result["replay"]["engine"] == "PAPER_EXIT_REPLAY_V1"
    assert result["replay"]["positions"] == 0
    assert result["execution_reconciliation"] == {"matched_exact_identity": 1}
    assert db.get_list_calls == []
    assert not any(call[0] == "replay_batch" for call in calls)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"limit": 0}, "limit должен быть от 1 до 100"),
        ({"portfolio_capital_rub": 0}, "portfolio_capital_rub"),
        ({"max_allocation_pct": 101}, "max_allocation_pct"),
    ],
)
def test_batch_replay_validates_limits(kwargs, message):
    runtime, *_ = make_runtime()
    with pytest.raises(HTTPException) as error:
        asyncio.run(runtime.replay_batch(**kwargs))
    assert error.value.status_code == 422
    assert message in error.value.detail


def test_batch_replay_rejects_missing_events_for_selected_sessions():
    trade = {
        "id": "P1", "mode": "PAPER", "status": "CLOSED",
        "ticker": "SBER", "bbo_replay_session_id": "S1",
    }
    runtime, *_ = make_runtime(db=DB(trades={"P1": trade}, events=[]))
    with pytest.raises(HTTPException) as error:
        asyncio.run(runtime.replay_batch())
    assert error.value.status_code == 409


def test_shadow_reports_delegate_to_analytics_with_validated_arguments():
    runtime, _, _, _, labels, _, calls = make_runtime()

    assert runtime.get_shadow_reconciliation(limit=12) == {
        "paper_mode": True, "matched": 1
    }
    assert runtime.get_shadow_walk_forward(
        folds=6, embargo_seconds=12.5, limit=100
    ) == {"paper_mode": True, "folds": []}
    assert runtime.get_shadow_cost_sensitivity(limit=99) == {
        "paper_mode": True, "scenarios": []
    }

    walk = next(row for row in calls if row[0] == "walk_forward")
    assert walk[2]["folds"] == 6
    assert walk[2]["embargo_seconds"] == 12.5
    assert walk[2]["max_horizon_seconds"] == 600
    cost = next(row for row in calls if row[0] == "shadow_cost")
    assert cost[2]["scenarios_bps"] == (0.0, 1.0, 2.0, 5.0)
    assert cost[1][0] is labels


def test_shadow_endpoints_validate_fold_embargo_and_retention_bounds():
    runtime, *_ = make_runtime()
    for kwargs in ({"folds": 2}, {"folds": 11}, {"embargo_seconds": 3601}, {"limit": 20001}):
        with pytest.raises(HTTPException) as error:
            runtime.get_shadow_walk_forward(**kwargs)
        assert error.value.status_code == 422
    with pytest.raises(HTTPException):
        runtime.get_shadow_cost_sensitivity(limit=20001)


def test_research_report_joins_read_only_sections_and_fixed_timestamp():
    runtime, _, _, _, _, _, calls = make_runtime()
    result = asyncio.run(runtime.get_research_report(limit=15))
    assert result["generated_at"] == "2026-11-21T12:00:00+00:00"
    assert result["limit"] == 15
    assert result["realized_scorecard"]["overall"]["trades"] == 1
    assert result["shadow_reconciliation"] == {"matched": 1}
    assert result["exit_replay"]["selected_positions"] == 0
    assert "none of these diagnostics changes active trading rules" in result["interpretation"]
    assert any(row[0] == "walk_forward" and row[2]["limit"] == 20000 for row in calls)
    assert any(row[0] == "shadow_cost" and row[2]["limit"] == 20000 for row in calls)


def test_single_replay_requires_paper_id_session_and_bbo_events():
    trade = {"id": "P1", "mode": "PAPER", "status": "CLOSED", "bbo_replay_session_id": ""}
    runtime, *_ = make_runtime(db=DB(trades={"P1": trade}))
    with pytest.raises(HTTPException) as error:
        asyncio.run(runtime.replay_trade("P1"))
    assert error.value.status_code == 409

    trade["bbo_replay_session_id"] = "S1"
    runtime, *_ = make_runtime(db=DB(trades={"P1": trade}, events=[]))
    with pytest.raises(HTTPException) as error:
        asyncio.run(runtime.replay_trade("P1"))
    assert error.value.status_code == 409


def test_single_replay_matches_journal_session_and_reconciles_closed_trade():
    trade = {
        "id": "P1", "mode": "PAPER", "status": "CLOSED",
        "bbo_replay_session_id": "S1",
    }
    events = [{"session_id": "S1"}, {"session_id": "OTHER"}]
    runtime, _, _, _, _, _, calls = make_runtime(
        db=DB(trades={"P1": trade}, events=events)
    )
    result = asyncio.run(runtime.replay_trade("P1"))
    call = next(row for row in calls if row[0] == "replay_one")
    assert len(call[1][0]) == 1
    assert call[2]["max_bbo_age_sec"] == 1.25
    assert result["execution_reconciliation"] == {"matched_exact_identity": 1}


def test_reconciliation_snapshot_filters_only_review_required_stale_rows():
    snapshot = {
        "paper_trades": [
            {"id": "a", "status": "STALE_REVIEW", "reconciliation_status": "REVIEW_REQUIRED"},
            {"id": "b", "status": "STALE_REVIEW", "reconciliation_status": "DONE"},
            {"id": "c", "status": "OPEN"},
        ],
        "paper_stale_count": 2,
    }
    engine = SimpleNamespace(
        paper_mode=True, _maintenance_active=True,
        paper_positions={"SBER": {"ticker": "SBER", "status": "OPEN"}},
    )
    runtime, *_ = make_runtime(db=DB(snapshot=snapshot), engine=engine)
    result = asyncio.run(runtime.get_reconciliation())
    assert result["maintenance_active"] is True
    assert result["summary"] == {"paper_stale_count": 2}
    assert [row["id"] for row in result["stale_positions"]] == ["a"]
    assert result["memory_open_positions"] == [{"ticker": "SBER", "status": "OPEN"}]


def stale_trade(**overrides):
    row = {
        "id": "P1", "mode": "PAPER", "status": "STALE_REVIEW",
        "reconciliation_status": "REVIEW_REQUIRED",
        "ticker": "SBER", "side": "BUY", "entry_price": 100.0,
        "volume": 10, "current_price": 102.0,
    }
    row.update(overrides)
    return row


def test_reconcile_stale_position_close_persists_cost_basis_pnl_and_broadcasts():
    db = DB(trades={"P1": stale_trade()})
    engine = SimpleNamespace(
        paper_mode=True, _opening_tickers=set(), _lock_closing=False,
        paper_positions={}, _unreconciled_paper_positions={"P1": stale_trade()},
        closed_trades=[],
    )
    runtime, _, _, _, _, manager, _ = make_runtime(db=db, engine=engine)
    result = asyncio.run(runtime.reconcile_position(
        "P1", {"action": "close", "exit_price": "110", "note": "operator review"}
    ))

    assert result["trade"]["status"] == "CLOSED"
    assert result["trade"]["pnl"] == 100.0
    assert result["trade"]["pnl_percent"] == 10.0
    assert result["trade"]["exit_reason"] == "MANUAL_RECONCILIATION"
    assert not engine._unreconciled_paper_positions
    assert engine.closed_trades[0]["id"] == "P1"
    assert db.history_rows[0][0] == "piluitus:trade_history"
    assert manager.broadcasts == 1


def test_reconcile_stale_position_restore_open_and_void():
    db = DB(trades={"P1": stale_trade(), "P2": stale_trade(id="P2")})
    engine = SimpleNamespace(
        paper_mode=True, _opening_tickers=set(), _lock_closing=False,
        paper_positions={}, _unreconciled_paper_positions={
            "P1": stale_trade(), "P2": stale_trade(id="P2"),
        }, closed_trades=[],
    )
    runtime, _, _, _, _, manager, _ = make_runtime(db=db, engine=engine)

    restored = asyncio.run(runtime.reconcile_position(
        "P1", {"action": "restore_open", "note": "confirmed"}
    ))
    assert restored["trade"]["reconciliation_status"] == "CONFIRMED_OPEN"
    assert engine.paper_positions["SBER"]["status"] == "OPEN"
    assert "P1" not in engine._unreconciled_paper_positions

    voided = asyncio.run(runtime.reconcile_position(
        "P2", {"action": "void", "note": "invalid record"}
    ))
    assert voided["trade"]["status"] == "VOIDED"
    assert voided["trade"]["reconciliation_status"] == "VOIDED_CONFIRMED"
    assert "P2" not in engine._unreconciled_paper_positions
    assert manager.broadcasts == 2


def test_reconcile_rejects_invalid_action_active_operation_and_missing_note():
    engine = SimpleNamespace(
        paper_mode=True, _opening_tickers=set(), _lock_closing=False,
        paper_positions={}, _unreconciled_paper_positions={}, closed_trades=[],
    )
    runtime, *_ = make_runtime(engine=engine)
    with pytest.raises(HTTPException) as error:
        asyncio.run(runtime.reconcile_position("P1", {"action": "delete"}))
    assert error.value.status_code == 400

    engine._opening_tickers.add("SBER")
    with pytest.raises(HTTPException) as error:
        asyncio.run(runtime.reconcile_position("P1", {"action": "void", "note": "valid"}))
    assert error.value.status_code == 409
    engine._opening_tickers.clear()

    runtime, *_ = make_runtime(db=DB(trades={"P1": stale_trade()}), engine=engine)
    with pytest.raises(HTTPException) as error:
        asyncio.run(runtime.reconcile_position("P1", {"action": "void", "note": "x"}))
    assert error.value.status_code == 422
