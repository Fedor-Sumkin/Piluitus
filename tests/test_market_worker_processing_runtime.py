
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



def test_queue_processor_runs_candidate_phases_in_order_and_acknowledges() -> None:
    from types import MethodType

    async def scenario() -> None:
        queue = asyncio.PriorityQueue()
        queue.put_nowait((
            1, 1, 1, "MARKET_TICK",
            {"ticker": "SBER", "paper_session_generation": 3},
        ))
        runtime, _, _logger, _risk, _engine, released, *_ = _runtime(queue=queue)
        calls = []

        async def prepare(self, event_payload):
            calls.append("admission")
            return {"event_payload": event_payload, "ticker": "SBER"}

        async def analyze(self, state):
            calls.append("llm")
            return True

        def build(self, state):
            calls.append("verdict")

        async def publish(self, state):
            calls.append("publish")

        runtime._prepare_market_candidate_state = MethodType(prepare, runtime)
        runtime._analyze_candidate_and_prepare_entry = MethodType(analyze, runtime)
        runtime._build_candidate_verdict = MethodType(build, runtime)
        runtime._publish_candidate_verdict = MethodType(publish, runtime)

        await runtime.process_next_item()
        await asyncio.wait_for(queue.join(), timeout=0.5)

        assert calls == ["admission", "llm", "verdict", "publish"]
        assert released == ["SBER"]
        assert queue.empty()

    asyncio.run(scenario())


def test_verdict_construction_keeps_all_subphases_in_original_order() -> None:
    runtime, *_rest = _runtime()
    calls = []

    def build_signal_reason(**_kwargs):
        calls.append("diagnostic_reason")
        return "LOCAL_REASON"

    def build_explanation(*_args):
        calls.append("diagnostic_explanation")
        return "TACTIC", "CRITIC"

    def classify_diagnostic(**_kwargs):
        calls.append("diagnostic_classification")
        return "LOCAL_GUARD"

    def build_record(**kwargs):
        calls.append("record")
        assert kwargs["signal_reason"] == "LOCAL_REASON"
        assert kwargs["tactic_text"] == "TACTIC"
        return {"signal_status": "APPROVED"}

    def link_shadow(*_args, **_kwargs):
        calls.append("shadow_link")

    def enrich_analysis(*_args, **_kwargs):
        calls.append("analysis_enrichment")

    def log_event(**_kwargs):
        calls.append("event_log")

        async def completed():
            return None

        return completed()

    runtime.build_default_signal_reason = build_signal_reason
    runtime.build_verdict_explanation = build_explanation
    runtime.classify_local_evidence_diagnostic = classify_diagnostic
    runtime.build_llm_verdict_record = build_record
    runtime.link_shadow_candidate_verdict = link_shadow
    runtime.enrich_ai_analysis_with_verdict = enrich_analysis
    runtime.log_trading_event = log_event

    state = {
        "event_payload": {"shadow_candidate_id": "shadow-1"},
        "ticker": "SBER",
        "price": 100.0,
        "metrics": {"direction_evidence_state": "READY"},
        "llm_input_features": {"spread": 0.1},
        "ai_analysis": {},
        "request_error": False,
        "request_error_type": "",
        "model_decision": "BUY",
        "candidate_direction": "BUY",
        "model_reason_code": "MOMENTUM",
        "candidate_decision": "BUY",
        "model_contract_violation": False,
        "decision": "BUY",
        "confidence_valid": True,
        "confidence": 80.0,
        "candidate_confidence": 80.0,
        "evidence": {"source": "local"},
        "block_reason": "",
        "post_llm_features": {},
        "post_llm_evidence_validation": "VALID",
        "post_llm_evidence_reason": "",
        "post_llm_refresh": {},
        "verdict_id": "verdict-1",
        "signal_reason": "",
        "execution_status": "PENDING",
        "execution_message": "",
        "requested_lots": 1,
        "requested_notional": 100.0,
    }

    runtime._build_candidate_verdict(state)

    assert calls == [
        "diagnostic_reason",
        "diagnostic_explanation",
        "diagnostic_classification",
        "record",
        "shadow_link",
        "analysis_enrichment",
        "event_log",
    ]
    assert state["signal_status"] == "APPROVED"
    assert state["shadow_candidate_id"] == "shadow-1"
    assert state["verdict_data"] == {"signal_status": "APPROVED"}
    assert state["tactic_text"] == "TACTIC"
    assert state["critic_text"] == "CRITIC"
    assert state["local_evidence_diagnostic"] == "LOCAL_GUARD"
