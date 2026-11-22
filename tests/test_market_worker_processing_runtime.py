
from __future__ import annotations

import asyncio
from types import SimpleNamespace

from runtime.market_worker_processing_runtime import MarketWorkerProcessingRuntime


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


class _BackgroundOperations:
    def spawn(self, coroutine, *, name):
        close = getattr(coroutine, "close", None)
        if callable(close):
            close()


class _RiskManager:
    def __init__(self):
        self.daily_loss_limit = None
        self.allowed = True
        self.calls = []

    async def is_trading_allowed(self, db):
        self.calls.append(db)
        return self.allowed


class _ExecutionEngine:
    _maintenance_active = False
    paper_mode = True

    async def close_all_positions(self, *, db):
        return None


def _runtime(*, queue=None, generation=3):
    queue = queue or asyncio.PriorityQueue()
    logger = _Logger()
    risk = _RiskManager()
    engine = _ExecutionEngine()
    released = []
    counters = []
    cache = []
    balance = {"equity": 100_000.0}
    published = []

    async def no_op_async(*args, **kwargs):
        return None

    async def publish_verdict(verdict, **kwargs):
        published.append((verdict, kwargs))

    runtime = MarketWorkerProcessingRuntime(
        db=object(),
        ai=SimpleNamespace(analyze_market_state=no_op_async),
        anomaly_queue=queue,
        execution_engine=engine,
        risk_manager=risk,
        session_lock=asyncio.Lock(),
        session_generation_provider=lambda: generation,
        candidate_is_expired=lambda *_args, **_kwargs: False,
        candidate_ttl_sec=5.0,
        trade_flow_increment=counters.append,
        logger=logger,
        settings=SimpleNamespace(
            DAILY_LOSS_LIMIT_PCT=5.0,
            VLLM_TIMEOUT=1.0,
            PAPER_TRADING=True,
        ),
        validate_daily_loss_limit=lambda value: float(value),
        parse_finite_float=lambda value: float(value) if value is not None else None,
        background_operations=_BackgroundOperations(),
        log_trading_event=no_op_async,
        attach_llm_rule_context=no_op_async,
        build_llm_feature_snapshot=lambda metrics: dict(metrics or {}),
        build_llm_error_response=lambda exc, *, error_type: {
            "request_error": True,
            "request_error_type": error_type,
        },
        process_llm_response=lambda *args, **kwargs: ({}, {}),
        validate_post_llm_entry=lambda **kwargs: {},
        screener=object(),
        queue_flow_conflict_reason=lambda *args, **kwargs: "",
        prepare_entry_execution_plan=lambda **kwargs: {},
        lot_sizes={},
        trade_flow_guard_block=lambda reason: None,
        finite_float=lambda value: float(value) if value is not None else None,
        nonnegative_int=lambda value: int(value) if value is not None else None,
        build_default_signal_reason=lambda **kwargs: "",
        build_verdict_explanation=lambda *args, **kwargs: ("", ""),
        classify_local_evidence_diagnostic=lambda **kwargs: "",
        build_pre_llm_risk_block_verdict=lambda *args, **kwargs: {},
        build_llm_verdict_record=lambda **kwargs: {},
        link_shadow_candidate_verdict=lambda *args, **kwargs: False,
        shadow_tracker=SimpleNamespace(update_execution_result=lambda *args, **kwargs: None),
        enrich_ai_analysis_with_verdict=lambda *args, **kwargs: None,
        verdict_cache=cache,
        max_cache_size=100,
        manager=SimpleNamespace(broadcast=no_op_async),
        balance_snapshot=balance,
        publish_market_verdict=publish_verdict,
        execute_and_publish=no_op_async,
        release_ai_ticker=released.append,
        paper_session_generation_lock=asyncio.Lock(),
        sleep=lambda _delay: no_op_async(),
        now_local=lambda: __import__("datetime").datetime.now().astimezone(),
    )
    return runtime, queue, logger, risk, engine, released, counters, published, cache


def test_processing_runtime_acknowledges_and_releases_stale_session_candidate():
    async def scenario():
        queue = asyncio.PriorityQueue()
        payload = {
            "ticker": "SBER",
            "paper_session_generation": 2,
            "candidate_created_monotonic": 100.0,
        }
        queue.put_nowait((1, 1, 1, "MARKET_TICK", payload))
        runtime, _, _logger, _risk, _engine, released, *_ = _runtime(
            queue=queue, generation=3
        )

        await runtime.process_next_item()
        await asyncio.wait_for(queue.join(), timeout=0.5)

        assert released == ["SBER"]
        assert queue.empty()

    asyncio.run(scenario())


def test_processing_runtime_handles_priority_risk_limit_command_without_llm():
    async def scenario():
        queue = asyncio.PriorityQueue()
        queue.put_nowait((0, 1, 1, "CMD_SET_RISK_LIMIT", {"value": 2.5}))
        runtime, _, _logger, risk, _engine, released, *_ = _runtime(queue=queue)

        await runtime.process_next_item()
        await asyncio.wait_for(queue.join(), timeout=0.5)

        assert risk.daily_loss_limit == 2.5
        assert released == []
        assert queue.empty()

    asyncio.run(scenario())


def test_processing_runtime_keeps_emergency_close_on_priority_queue():
    async def scenario():
        queue = asyncio.PriorityQueue()
        queue.put_nowait((0, 1, 1, "CMD_CLOSE_ALL", {}))
        runtime, _, _logger, _risk, engine, released, *_ = _runtime(queue=queue)

        await runtime.process_next_item()
        await asyncio.wait_for(queue.join(), timeout=0.5)

        assert released == []
        assert queue.empty()

    asyncio.run(scenario())

def test_processing_runtime_rejects_pre_llm_risk_verdict_after_session_change():
    async def scenario():
        runtime, *_rest, cache = _runtime(generation=4)

        await runtime.publish_pre_llm_risk_block(
            {"ticker": "SBER", "paper_session_generation": 3},
            session_generation=3,
        )

        assert cache == []

    asyncio.run(scenario())


def test_processing_runtime_rate_limits_pre_llm_risk_verdicts():
    async def scenario():
        runtime, *_rest, _published, cache = _runtime()
        runtime.monotonic = lambda: 100.0
        runtime.build_pre_llm_risk_block_verdict = (
            lambda *args, **kwargs: {"ticker": "SBER"}
        )

        await runtime.publish_pre_llm_risk_block_locked({"ticker": "SBER"})
        await runtime.publish_pre_llm_risk_block_locked({"ticker": "GAZP"})

        assert cache == [{"ticker": "SBER"}]

    asyncio.run(scenario())

