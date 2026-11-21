from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from execution.market_worker_execution_runtime import MarketWorkerExecutionRuntime


class _Logger:
    def __init__(self):
        self.infos = []
        self.warnings = []
        self.errors = []

    def info(self, *args, **kwargs):
        self.infos.append((args, kwargs))

    def warning(self, *args, **kwargs):
        self.warnings.append((args, kwargs))

    def error(self, *args, **kwargs):
        self.errors.append((args, kwargs))


class _ExecutionEngine:
    paper_mode = True
    _maintenance_active = False

    def __init__(self, result=None):
        self.result = result or {"status": "OPENED", "mode": "PAPER", "lots": 2}
        self.calls = []

    async def execute_order(self, **kwargs):
        self.calls.append(kwargs)
        return self.result


class _RiskManager:
    def __init__(self, allowed=True):
        self.allowed = allowed
        self.calls = []

    async def is_trading_allowed(self, db, *, force_refresh=False):
        self.calls.append((db, force_refresh))
        return self.allowed


class _ShadowTracker:
    def __init__(self):
        self.updates = []

    def update_execution_result(self, candidate_id, *, execution_status):
        self.updates.append((candidate_id, execution_status))


def _build_runtime(*, allowed=True, generation=7, result=None):
    engine = _ExecutionEngine(result=result)
    risk = _RiskManager(allowed=allowed)
    logger = _Logger()
    shadow = _ShadowTracker()
    increments = []
    published = []
    fill_calls = []

    async def publish(update, **kwargs):
        published.append((update, kwargs))

    def normalize_fill(result, **kwargs):
        fill_calls.append((result, kwargs))
        return {
            "order_id": result.get("order_id", "ORDER-1"),
            "actual_lots": result.get("lots", 2),
            "actual_shares": result.get("shares", 20),
            "actual_notional": result.get("notional", 2000.0),
            "requested_lots": result.get("requested_lots", kwargs["requested_lots"]),
        }

    runtime = MarketWorkerExecutionRuntime(
        db=object(),
        client_id="PAPER-CLIENT",
        execution_engine=engine,
        risk_manager=risk,
        session_lock=asyncio.Lock(),
        session_generation_provider=lambda: generation,
        candidate_is_expired=lambda *_args, **_kwargs: False,
        candidate_ttl_sec=4.0,
        trade_flow_increment=increments.append,
        logger=logger,
        shadow_tracker=shadow,
        normalise_execution_status=lambda status, _mode: status,
        nonnegative_int=lambda value: (
            int(value) if value is not None and str(value).lstrip("-").isdigit() else None
        ),
        normalize_execution_fill=normalize_fill,
        lot_sizes={"SBER": 10},
        publish_execution_outcome=publish,
        verdict_cache=[],
        get_open_positions=lambda: [],
        refresh_balance_snapshot=lambda: {},
        manager=object(),
        monotonic=lambda: 100.0,
        now_local=lambda: datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    return runtime, engine, risk, logger, shadow, increments, published, fill_calls


def _execute(runtime, *, session_generation=7, shadow_candidate_id="SHADOW-1"):
    asyncio.run(
        runtime.execute_and_publish(
            verdict_id="VERDICT-1",
            ticker="SBER",
            lots=2,
            side="BUY",
            price=100.0,
            ai_analysis={"source": "test"},
            session_generation=session_generation,
            candidate_created_monotonic=99.0,
            shadow_candidate_id=shadow_candidate_id,
        )
    )


def test_execution_runtime_rejects_signal_from_previous_paper_session():
    runtime, engine, _risk, logger, _shadow, _counts, published, _fills = _build_runtime(
        generation=8
    )

    _execute(runtime, session_generation=7)

    assert engine.calls == []
    assert published == []
    assert logger.infos
    assert "PAPER-сессия изменилась" in logger.infos[0][0][0]


def test_execution_runtime_rechecks_risk_before_calling_executor():
    runtime, engine, risk, _logger, shadow, counts, published, fill_calls = _build_runtime(
        allowed=False
    )

    _execute(runtime)

    assert engine.calls == []
    assert risk.calls and risk.calls[0][1] is True
    assert "risk_final_gate_blocked" in counts
    assert shadow.updates == [("SHADOW-1", "BLOCKED")]
    assert len(fill_calls) == 1
    assert len(published) == 1
    update = published[0][0]
    assert update["execution_status"] == "BLOCKED"
    assert "RISK_FINAL_GATE" in update["execution_message"]


def test_execution_runtime_normalizes_and_publishes_successful_paper_fill():
    runtime, engine, risk, _logger, shadow, counts, published, fill_calls = _build_runtime(
        result={
            "status": "OPENED",
            "mode": "PAPER",
            "lots": 2,
            "requested_lots": 2,
            "shares": 20,
            "notional": 2000.0,
            "order_id": "PAPER-ORDER-1",
            "message": "opened",
        }
    )

    _execute(runtime)

    assert len(engine.calls) == 1
    assert engine.calls[0]["client_id"] == "PAPER-CLIENT"
    assert engine.calls[0]["ticker"] == "SBER"
    assert risk.calls and risk.calls[0][1] is True
    assert shadow.updates == [("SHADOW-1", "OPENED")]
    assert "orders_scheduled" in counts
    assert "orders_opened_buy" in counts
    assert len(fill_calls) == 1
    assert len(published) == 1
    update = published[0][0]
    assert update["execution_status"] == "OPENED"
    assert update["order_id"] == "PAPER-ORDER-1"
    assert update["executed_lots"] == 2
    assert update["executed_shares"] == 20
    assert update["actual_notional_rub"] == 2000.0

def test_execution_runtime_blocks_candidate_that_expired_while_queued():
    runtime, engine, _risk, _logger, _shadow, counts, published, _fills = _build_runtime()
    runtime.candidate_is_expired = lambda *_args, **_kwargs: True

    _execute(runtime)

    assert engine.calls == []
    assert "execution_stale_signal_blocked" in counts
    assert len(published) == 1
    assert published[0][0]["execution_status"] == "BLOCKED"
    assert "SIGNAL_EXPIRED" in published[0][0]["execution_message"]

