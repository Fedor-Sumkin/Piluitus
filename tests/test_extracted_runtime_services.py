from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from interface.paper_api_helpers import PaperAPIHelpers
from portfolio.dashboard_runtime import DashboardRuntime
from storage.paper_session_runtime import PaperSessionRuntime
from strategy.market_data_runtime import (
    M1HistoryWarmup,
    PendingBBOCandidateStore,
    paper_queue_flow_conflict_reason,
)


class Logger:
    def __init__(self):
        self.messages = []

    def info(self, *args, **kwargs):
        self.messages.append(("info", args))

    def warning(self, *args, **kwargs):
        self.messages.append(("warning", args))

    def debug(self, *args, **kwargs):
        self.messages.append(("debug", args))

    def error(self, *args, **kwargs):
        self.messages.append(("error", args))

    def critical(self, *args, **kwargs):
        self.messages.append(("critical", args))


def test_pending_bbo_store_coalesces_without_restarting_ttl():
    now = [10.0]
    increments = []
    store = PendingBBOCandidateStore(
        2.0, lambda key, amount: increments.append((key, amount)),
        Logger(), monotonic=lambda: now[0],
    )
    store.store("sber@TQBR", {"candidate_direction": "BUY", "metrics": {"bid": 1}})
    first = store.candidates["SBER"]
    now[0] = 11.0
    store.store("SBER", {"candidate_direction": "BUY", "metrics": {"bid": 2}})
    assert store.candidates["SBER"] is first
    assert first["created_at"] == 10.0
    assert first["payload"]["metrics"]["bid"] == 2
    assert ("bbo_coalesced", 1) in increments
    assert store.is_current("SBER", first, first["payload"])


def test_pending_bbo_store_supersedes_cancels_and_expires():
    now = [20.0]
    increments = []
    store = PendingBBOCandidateStore(
        1.5, lambda key, amount: increments.append((key, amount)),
        Logger(), monotonic=lambda: now[0],
    )
    store.store("GAZP", {"candidate_direction": "BUY"})
    first = store.candidates["GAZP"]
    now[0] = 20.5
    store.store("GAZP", {"candidate_direction": "SELL"})
    second = store.candidates["GAZP"]
    assert second is not first
    assert ("bbo_superseded", 1) in increments
    assert store.cancel("GAZP", first) is False
    assert store.cancel("GAZP", second) is True
    store.store("SBER", {"candidate_direction": "BUY"})
    store.expire(now=22.1)
    assert store.count() == 0
    assert ("bbo_expired", 1) in increments


def test_paper_queue_flow_guard_only_blocks_enabled_paper_conflicts():
    conflict = lambda direction, metrics: True
    number = lambda value: float(value) if value is not None else None
    metrics = {"queue_imbalance_5s": -0.5, "trade_flow_imbalance_30s": 0.7}
    reason = paper_queue_flow_conflict_reason(
        "BUY", metrics, paper_mode=True, enabled="1",
        conflict_detector=conflict, finite_float=number,
    )
    assert "queue=-0.500" in reason and "flow=+0.700" in reason
    assert paper_queue_flow_conflict_reason(
        "BUY", metrics, paper_mode=False, enabled="1",
        conflict_detector=conflict, finite_float=number,
    ) == ""
    assert paper_queue_flow_conflict_reason(
        "BUY", metrics, paper_mode=True, enabled="0",
        conflict_detector=conflict, finite_float=number,
    ) == ""


def test_m1_history_warmup_normalizes_ticker_and_clears_pending():
    class Broker:
        def __init__(self):
            self.calls = []

        async def get_historical_m1_bars(self, ticker, lookback_minutes):
            self.calls.append((ticker, lookback_minutes))
            return []

    class Patterns:
        def load_closed_bars(self, ticker, bars):
            return 0

    class Screener:
        minute_bar_patterns = Patterns()

    broker = Broker()
    logger = Logger()
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    warmup = M1HistoryWarmup(
        broker, Screener(), logger, retry_sec=0,
        monotonic=lambda: 100.0, sleep=fake_sleep,
    )
    asyncio.run(warmup.warmup_selected_m1_history([" sber@TQBR ", ""]))
    assert broker.calls == [("SBER", 120)]
    assert warmup.pending == set()
    assert warmup.last_attempt == {"SBER": 100.0}
    assert sleeps == [pytest.approx(0.36)]


def test_dashboard_runtime_reads_cost_basis_open_pnl_and_prices():
    engine = SimpleNamespace(
        paper_mode=True,
        paper_positions={"SBER": {
            "ticker": "SBER", "status": "OPEN", "volume": 10,
            "entry_price": 100.0, "current_price": 150.0, "pnl": 20.0,
        }},
        _unreconciled_paper_positions={"old": {
            "ticker": "GAZP", "status": "STALE_REVIEW",
            "reconciliation_status": "REVIEW_REQUIRED",
            "volume": 5, "entry_price": 200.0,
        }},
    )
    screener = SimpleNamespace(history={
        "sber@TQBR": {"count": 2, "prices": [100.0, 101.5]},
        "GAZP": {"count": 1, "prices": [float("nan")]},
    })
    runtime = DashboardRuntime(
        engine, SimpleNamespace(), screener, {"SBER": 1, "GAZP": 1}, Logger()
    )
    assert runtime.current_paper_open_pnl() == 20.0
    assert runtime.current_paper_assets_cost() == 2_000.0
    assert runtime.latest_screener_prices() == {"SBER": 101.5}


def test_dashboard_runtime_builds_cost_basis_snapshot_and_resets_guard():
    engine = SimpleNamespace(
        paper_mode=True, max_gross_exposure_pct=0.15, max_single_position_pct=0.05,
        _unreconciled_paper_positions={"legacy": {
            "id": "legacy", "ticker": "GAZP", "status": "STALE_REVIEW",
            "reconciliation_status": "REVIEW_REQUIRED",
            "volume": 5, "entry_price": 200.0,
        }},
    )
    logger = Logger()
    runtime = DashboardRuntime(
        engine, SimpleNamespace(), SimpleNamespace(history={}),
        {"SBER": 1, "GAZP": 1}, logger,
    )
    risk = SimpleNamespace(
        initial_deposit=100_000.0, total_session_pnl=1_500.0,
        realized_pnl=500.0, open_pnl=1_000.0,
    )
    snapshot = asyncio.run(runtime.refresh_balance_snapshot(
        [{
            "id": "active", "ticker": "SBER", "status": "OPEN",
            "volume": 25, "entry_price": 100.0,
        }],
        risk_manager=risk, initial_deposit=100_000.0,
    ))
    assert snapshot["margin_used"] == 3_500.0
    assert snapshot["free_margin"] == 97_000.0
    assert snapshot["equity"] == 101_500.0
    assert snapshot["unreconciled_positions_count"] == 1
    assert any(level == "critical" for level, _ in logger.messages)
    runtime.reset_balance_guard_signature()
    assert runtime._balance_guard_last_signature is None


def test_dashboard_runtime_reads_only_open_live_positions():
    class DB:
        async def get_active_positions(self):
            return {
                "a": {"ticker": "SBER", "status": "OPEN"},
                "b": {"ticker": "GAZP", "status": "CLOSED"},
            }

    runtime = DashboardRuntime(
        SimpleNamespace(paper_mode=False), DB(),
        SimpleNamespace(history={}), {}, Logger(),
    )
    assert asyncio.run(runtime.get_dashboard_open_positions()) == [
        {"ticker": "SBER", "status": "OPEN"}
    ]


def test_paper_api_helpers_check_mode_filter_rows_and_broadcast():
    class Manager:
        def __init__(self):
            self.messages = []

        async def broadcast(self, payload):
            self.messages.append(json.loads(payload))

    engine = SimpleNamespace(
        paper_mode=True, closed_trades=[{"id": i} for i in range(35)]
    )
    manager = Manager()
    positions = [{"ticker": "SBER"}]
    balance = {"free_margin": 123.0}

    async def get_positions():
        return positions

    async def refresh(received):
        assert received is positions
        return balance

    helpers = PaperAPIHelpers(engine, manager, get_positions, refresh)
    helpers.require_paper_mode()
    assert helpers.paper_trade_rows({
        "paper_trades": [{"id": 1}, None, "invalid", {"id": 2}]
    }) == [{"id": 1}, {"id": 2}]
    asyncio.run(helpers.broadcast_paper_tables())
    payload = manager.messages[0]
    assert payload["type"] == "TABLES_UPDATE"
    assert payload["closed_trades"] == engine.closed_trades[-30:]
    assert payload["balance"] == balance

    engine.paper_mode = False
    with pytest.raises(HTTPException) as error:
        helpers.require_paper_mode()
    assert error.value.status_code == 403


class ShadowPersistence:
    def __init__(self):
        self.persisted = []
        self.restored = []

    async def persist_completed_labels(self, labels, *, session_id, source):
        self.persisted.append((labels, session_id, source))

    async def restore_completed_labels(self, *, paper_mode, session_id):
        self.restored.append((paper_mode, session_id))


class ReplayRecorder:
    def __init__(self, session_id):
        self.session_id = session_id
        self.closed = False
        self.started_with = None
        self.record_exit_trigger = lambda *args: None

    async def close(self):
        self.closed = True

    def start(self, db_manager):
        self.started_with = db_manager


class PerformanceTracker:
    def __init__(self):
        self.records = []
        self.restored = None

    def restore(self, rows):
        self.records = list(rows)
        self.restored = list(rows)
        return len(rows)

    def scorecard(self, labels):
        return {"overall": {
            "trades": len(self.records), "net_pnl_rub": 10.0,
            "win_rate_pct": 50.0, "profit_factor": 1.2,
            "max_drawdown_rub": 3.0,
        }}


def make_session_runtime(db, engine=None, tracker=None, logger=None):
    return PaperSessionRuntime(
        db, engine or SimpleNamespace(session_id="s-7", paper_mode=True),
        ShadowPersistence(), ReplayRecorder("old"),
        lambda: ReplayRecorder("new"), tracker or PerformanceTracker(),
        [], logger or Logger(),
        closed_trades_storage_key="paper:closed",
        closed_trades_storage_limit=50,
    )


def test_paper_session_runtime_persists_and_restores_shadow_labels():
    runtime = make_session_runtime(object())
    labels = [{"candidate_id": "c1"}]
    asyncio.run(runtime.persist_shadow_outcomes(labels, source="session"))
    asyncio.run(runtime.restore_shadow_outcomes())
    assert runtime.shadow_outcome_persistence.persisted == [
        (labels, "s-7", "session")
    ]
    assert runtime.shadow_outcome_persistence.restored == [(True, "s-7")]


def test_paper_session_runtime_rotates_replay_recorder():
    db = object()
    runtime = make_session_runtime(db, engine=SimpleNamespace())
    old = runtime.bbo_replay_recorder
    new = asyncio.run(runtime.rotate_bbo_replay_session())
    assert old.closed is True
    assert runtime.bbo_replay_recorder is new
    assert new.session_id == "new"
    assert new.started_with is db
    assert runtime.execution_engine.bbo_replay_session_id == "new"
    assert runtime.execution_engine.paper_exit_event_recorder == new.record_exit_trigger


def test_paper_session_runtime_restores_and_migrates_legacy_performance():
    class DB:
        def __init__(self):
            self.calls = []
            self.pushed = None

        async def get_list(self, key, *, start, stop):
            self.calls.append((key, start, stop))
            return [] if key == "paper:closed" else [{"trade_id": 1}]

        async def push_many_to_list(self, key, rows, *, max_len):
            self.pushed = (key, list(rows), max_len)

    db = DB()
    tracker = PerformanceTracker()
    logger = Logger()
    runtime = make_session_runtime(db, tracker=tracker, logger=logger)
    asyncio.run(runtime.restore_paper_performance())
    assert db.calls == [("paper:closed", 0, 49), ("piluitus:trade_history", 0, 99)]
    assert db.pushed == ("paper:closed", [{"trade_id": 1}], 50)
    assert tracker.restored == [{"trade_id": 1}]
    assert any("[PAPER PERFORMANCE RESTORE]" in args[0]
               for level, args in logger.messages if level == "info")
