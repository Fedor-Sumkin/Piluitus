from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from storage.paper_session_maintenance_runtime import (
    PAPER_CLEAR_CONFIRMATION,
    PaperSessionMaintenanceRuntime,
)


class Logger:
    def __init__(self):
        self.messages = []

    def warning(self, *args, **kwargs):
        self.messages.append(("warning", args))

    def critical(self, *args, **kwargs):
        self.messages.append(("critical", args))


class FakeDB:
    def __init__(self, snapshots=None):
        self.maintenance_writes = []
        self.snapshots = list(snapshots or [])
        self.clear_result = {"deleted": 4}
        self.stale_result = {
            "position_ids": ["stale-1"],
            "deleted_stale_positions": 1,
        }
        self.clear_calls = 0
        self.stale_calls = 0

    async def set_json(self, key, value):
        self.maintenance_writes.append((key, dict(value)))

    async def clear_paper_session(self):
        self.clear_calls += 1
        return self.clear_result

    async def clear_unreconciled_paper_positions(self):
        self.stale_calls += 1
        return self.stale_result

    async def get_paper_session_snapshot(self):
        return self.snapshots.pop(0)


def snapshot(**overrides):
    result = {
        "paper_stale_count": 0,
        "live_trade_count": 0,
        "live_history_count": 0,
        "live_active_order_count": 0,
        "paper_history_count": 12,
        "verdict_history_count": 9,
        "verdict_execution_count": 8,
    }
    result.update(overrides)
    return result


def make_runtime(*, db=None, paper_mode=True):
    db = db or FakeDB()
    engine = SimpleNamespace(
        paper_mode=paper_mode,
        _maintenance_active=False,
        _maintenance_reason="",
        _opening_tickers=set(),
        _lock_closing=False,
        _monitor_worker_task=None,
        _pending_monitor_prices={"SBER": 100.0},
        paper_positions={"SBER": {"ticker": "SBER", "status": "OPEN"}},
        closed_trades=[{"id": "closed-1"}],
        _unreconciled_paper_positions={
            "stale-1": {"status": "STALE_REVIEW"},
            "stale-2": {"status": "STALE_REVIEW"},
        },
        _ticker_cooldowns={"SBER": 10.0},
        _recent_stop_losses={"SBER": 1},
        _hydrated_stop_loss_keys={"SBER"},
        _reserved_notional={"SBER": 1000.0},
        _reserved_stop_risk={"SBER": 10.0},
    )
    risk = SimpleNamespace(
        _balance_initialized=True,
        block_trading=True,
        _blocked_date="today",
        total_session_pnl=-10.0,
        realized_pnl=-8.0,
        open_pnl=-2.0,
        current_equity=90.0,
        free_margin=50.0,
        high_water_mark=100.0,
        current_drawdown_pct=10.0,
        _drawdown_warning_active=True,
        _loss_streak_count=4,
        _loss_pause_signature="signature",
        _cached_trading_allowed=False,
        _last_risk_snapshot_monotonic=42.0,
    )
    calls = {
        "generation": 0,
        "balance_guard_resets": 0,
        "performance_clear": 0,
        "shadow_clear": 0,
        "dedup_clear": 0,
        "counter_resets": [],
        "position_diagnostics_clear": 0,
        "rotations": 0,
        "refresh_args": [],
        "table_broadcasts": 0,
        "reset_broadcasts": [],
    }

    class Tracker:
        def clear(self):
            calls["performance_clear"] += 1
            return 7

    class ShadowTracker:
        def clear(self):
            calls["shadow_clear"] += 1
            return 3

    class Deduplicator:
        def clear(self):
            calls["dedup_clear"] += 1

    class Counters:
        def reset(self, now):
            calls["counter_resets"].append(now)

    class PositionDiagnostics:
        def clear(self):
            calls["position_diagnostics_clear"] += 1

    class Manager:
        async def tables(self):
            calls["table_broadcasts"] += 1

        async def reset(self, payload):
            calls["reset_broadcasts"].append(payload)

    manager = Manager()
    queue = asyncio.PriorityQueue()
    queue.put_nowait((0, 1, "CMD_CLOSE_ALL", {}))
    queue.put_nowait((1, 2, "MARKET_TICK", {}))
    pending = {"GAZP": {"payload": {}}}
    labels = [{"candidate_id": "label-1"}]
    verdicts = [{"verdict_id": "verdict-1"}]
    logger = Logger()
    manager_reset = manager.reset
    manager_tables = manager.tables

    async def rotate():
        calls["rotations"] += 1

    async def refresh(positions):
        calls["refresh_args"].append(positions)
        return {"free_margin": 400000.0}

    def advance_generation():
        calls["generation"] += 1

    def reset_balance_guard():
        calls["balance_guard_resets"] += 1

    def require_paper():
        if not engine.paper_mode:
            raise HTTPException(
                status_code=403,
                detail="Операция доступна только в PAPER_TRADING=True.",
            )

    runtime = PaperSessionMaintenanceRuntime(
        db_manager=db,
        execution_engine=engine,
        risk_manager=risk,
        paper_session_lock=asyncio.Lock(),
        anomaly_queue=queue,
        ai_candidate_deduplicator=Deduplicator(),
        paper_performance_tracker=Tracker(),
        shadow_tracker=ShadowTracker(),
        paper_shadow_labels=labels,
        last_ai_verdicts_cache=verdicts,
        trade_flow_counters=Counters(),
        pending_bbo_candidates=pending,
        pending_bbo_lock=__import__("threading").Lock(),
        position_order_book_diagnostics=PositionDiagnostics(),
        require_paper_mode=require_paper,
        advance_session_generation=advance_generation,
        reset_balance_guard_signature=reset_balance_guard,
        rotate_bbo_replay_session=rotate,
        refresh_dashboard_balance_snapshot=refresh,
        broadcast_paper_tables=manager_tables,
        broadcast_paper_session_reset=manager_reset,
        get_bbo_replay_session_id=lambda: "replay-new",
        deposit=400000.0,
        logger=logger,
        now=lambda: datetime(2026, 11, 22, tzinfo=timezone.utc),
        monotonic=lambda: 123.5,
    )
    return runtime, engine, risk, db, queue, pending, labels, verdicts, calls, logger


def test_clear_requires_exact_confirmation_before_mutating_state():
    runtime, engine, _, db, _, _, _, _, calls, _ = make_runtime()

    with pytest.raises(HTTPException) as error:
        asyncio.run(runtime.clear_paper_session({"confirmation": "очистить"}))

    assert error.value.status_code == 400
    assert engine._maintenance_active is False
    assert db.maintenance_writes == []
    assert calls["generation"] == 0


def test_clear_resets_paper_state_and_preserves_emergency_queue_commands():
    (
        runtime, engine, risk, db, queue, pending, labels, verdicts, calls, _
    ) = make_runtime()

    result = asyncio.run(
        runtime.clear_paper_session({"confirmation": PAPER_CLEAR_CONFIRMATION})
    )

    assert result["status"] == "ok"
    assert result["maintenance_active"] is True
    assert result["memory"] == {
        "removed_open_positions": 1,
        "removed_closed_trades": 1,
        "removed_performance_records": 7,
        "removed_stale_positions": 2,
    }
    assert result["shadow_pending_discarded"] == 3
    assert result["shadow_labels_discarded"] == 1
    assert result["bbo_replay_session_id"] == "replay-new"
    assert result["balance"] == {"free_margin": 400000.0}
    assert engine._maintenance_active is True
    assert engine.paper_positions == {}
    assert engine.closed_trades == []
    assert engine._unreconciled_paper_positions == {}
    assert engine._ticker_cooldowns == {}
    assert engine._recent_stop_losses == {}
    assert engine._hydrated_stop_loss_keys == set()
    assert engine._reserved_notional == {}
    assert engine._reserved_stop_risk == {}
    assert engine._pending_monitor_prices == {}
    assert risk.current_equity == 400000.0
    assert risk.free_margin == 400000.0
    assert risk.total_session_pnl == 0.0
    assert risk.realized_pnl == 0.0
    assert risk.open_pnl == 0.0
    assert risk.block_trading is False
    assert risk._loss_streak_count == 0
    assert labels == []
    assert verdicts == []
    assert pending == {}
    assert calls["generation"] == 1
    assert calls["balance_guard_resets"] == 1
    assert calls["performance_clear"] == 1
    assert calls["shadow_clear"] == 1
    assert calls["dedup_clear"] == 1
    assert calls["counter_resets"] == [123.5]
    assert calls["position_diagnostics_clear"] == 1
    assert calls["rotations"] == 1
    assert calls["refresh_args"] == [[]]
    assert calls["reset_broadcasts"][0]["type"] == "PAPER_SESSION_RESET"
    assert db.maintenance_writes[0][1]["active"] is True
    assert queue.qsize() == 1
    assert queue.get_nowait()[0] == 0


def test_cleanup_stale_and_resume_preserves_open_positions_and_emergency_commands():
    db = FakeDB(snapshots=[snapshot(), snapshot()])
    (
        runtime, engine, _, db, queue, _, _, _, calls, _
    ) = make_runtime(db=db)
    result = asyncio.run(runtime.cleanup_stale_and_resume())

    assert result["status"] == "ok"
    assert result["maintenance_active"] is False
    assert result["memory"]["removed_stale_positions"] == 2
    assert result["preserved"]["active_memory_positions"] == 1
    assert engine.paper_positions == {"SBER": {"ticker": "SBER", "status": "OPEN"}}
    assert engine.closed_trades == [{"id": "closed-1"}]
    assert engine._unreconciled_paper_positions == {}
    assert engine._maintenance_active is False
    assert db.stale_calls == 1
    assert [item[1]["active"] for item in db.maintenance_writes] == [True, False]
    assert calls["generation"] == 1
    assert calls["balance_guard_resets"] == 1
    assert calls["table_broadcasts"] == 1
    assert queue.qsize() == 1
    assert queue.get_nowait()[0] == 0


def test_cleanup_stale_refuses_live_storage_records_without_mutating_state():
    db = FakeDB(snapshots=[snapshot(live_trade_count=1)])
    runtime, engine, _, db, _, _, _, _, calls, _ = make_runtime(db=db)

    with pytest.raises(HTTPException) as error:
        asyncio.run(runtime.cleanup_stale_and_resume())

    assert error.value.status_code == 409
    assert engine._maintenance_active is False
    assert db.stale_calls == 0
    assert db.maintenance_writes == []
    assert calls["generation"] == 0


def test_manual_resume_refuses_unreconciled_stale_positions():
    db = FakeDB(snapshots=[snapshot(paper_stale_count=1)])
    runtime, engine, _, db, _, _, _, _, _, _ = make_runtime(db=db)
    engine._maintenance_active = True
    engine._maintenance_reason = "paused"

    with pytest.raises(HTTPException) as error:
        asyncio.run(runtime.resume_paper_session())

    assert error.value.status_code == 409
    assert engine._maintenance_active is True
    assert db.maintenance_writes == []
