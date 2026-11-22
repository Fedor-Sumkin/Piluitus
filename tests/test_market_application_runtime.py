"""Contracts for the market application composition root."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

from runtime import market_application_runtime as module


class _Deduplicator:
    def __init__(self) -> None:
        self.reserved = []
        self.released = []

    def reserve(self, ticker, direction, pattern):
        self.reserved.append((ticker, direction, pattern))
        return True

    def release(self, ticker):
        self.released.append(ticker)


class _Warmup:
    def __init__(self) -> None:
        self.scheduled = []
        self.warmed = []

    def schedule(self, tickers):
        self.scheduled.append(tickers)

    async def warmup_selected_m1_history(self, tickers):
        self.warmed.append(tickers)


class _PendingStore:
    def __init__(self) -> None:
        self.stored = []

    def store_candidate(self, ticker, payload, *, received_monotonic=None):
        self.stored.append((ticker, payload, received_monotonic))


class _PendingRuntime:
    def __init__(self) -> None:
        self.dropped = []
        self.cancelled = []
        self.expired = []
        self.resumed = []

    def drop(self, ticker, expected=None):
        self.dropped.append((ticker, expected))
        return True

    def cancel(self, ticker, expected=None):
        self.cancelled.append((ticker, expected))
        return True

    def expire(self, now=None):
        self.expired.append(now)

    async def resume(self, ticker, *, trigger="ORDER_BOOK"):
        self.resumed.append((ticker, trigger))


class _EventRuntime:
    def __init__(self) -> None:
        self.events = []

    async def process_event(self, event):
        self.events.append(event)


class _IngestionFactory:
    def __init__(self, *, dependencies, callbacks):
        self.dependencies = dependencies
        self.callbacks = callbacks

    def create(self):
        self.dedup = _Deduplicator()
        self.warmup = _Warmup()
        self.store = _PendingStore()
        self.pending = _PendingRuntime()
        self.events = _EventRuntime()
        return SimpleNamespace(
            ai_candidate_deduplicator=self.dedup,
            ai_candidate_ttl_sec=5.0,
            m1_history_warmup=self.warmup,
            pending_bbo_candidate_store=self.store,
            pending_bbo_runtime=self.pending,
            market_data_event_runtime=self.events,
            pending_bbo_candidates={},
            pending_bbo_lock=object(),
            market_auxiliary_event_router=object(),
            market_candidate_pipeline=object(),
        )


class _WorkerFactory:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class _BackgroundServices:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


def _inputs():
    dependency_names = (
        "broker", "screener", "execution_engine", "db_manager",
        "anomaly_queue", "queue_item_factory", "shadow_tracker", "logger",
        "terminal_status", "recorder_provider", "session_generation_provider",
        "client_id", "ai_engine", "risk_manager", "session_lock",
        "background_operations", "manager", "get_open_positions",
        "refresh_balance_snapshot", "refresh_dashboard_balance_snapshot",
        "verdict_cache", "max_cache_size", "balance_snapshot", "tickers",
        "finam_trade_lot_sizes",
    )
    dependencies = {key: object() for key in dependency_names}
    dependencies.update({
        "environment": {"PILUITUS_PAPER_QUEUE_FLOW_CONFLICT_GUARD": "1"},
        "settings": SimpleNamespace(PAPER_TRADING=True),
        "monotonic": lambda: 100.0,
    })
    callback_names = (
        "trade_flow_increment", "guard_reason_bucket",
        "record_position_order_book_event", "refresh_bbo_metrics",
        "persist_shadow_outcomes", "infer_candidate_direction",
        "paper_mode_provider", "validate_entry_evidence", "validate_atr_readiness",
        "mark_shadow_llm_not_called", "log_shadow_candidate",
        "trade_flow_guard_block", "log_prefilter_blocked",
        "update_numpy_impulse_status", "build_candidate_payload",
        "classify_direction", "candidate_is_expired",
        "normalise_execution_status", "nonnegative_int",
        "normalize_execution_fill", "publish_execution_outcome",
        "validate_daily_loss_limit", "parse_finite_float", "log_trading_event",
        "attach_llm_rule_context", "build_llm_feature_snapshot",
        "build_llm_error_response", "process_llm_response",
        "validate_post_llm_entry", "prepare_entry_execution_plan",
        "finite_float", "build_default_signal_reason", "build_verdict_explanation",
        "classify_local_evidence_diagnostic", "build_pre_llm_risk_block_verdict",
        "build_llm_verdict_record", "link_shadow_candidate_verdict",
        "enrich_ai_analysis_with_verdict", "publish_market_verdict",
    )
    callbacks = {name: (lambda *args, **kwargs: None) for name in callback_names}
    callbacks["finite_float"] = lambda value: 0.0
    return dependencies, callbacks


def test_market_application_runtime_composes_all_three_market_layers(monkeypatch):
    monkeypatch.setattr(module, "MarketIngestionRuntimeFactory", _IngestionFactory)
    monkeypatch.setattr(module, "MarketWorkerFactory", _WorkerFactory)
    monkeypatch.setattr(module, "MarketBackgroundServicesRuntime", _BackgroundServices)
    dependencies, callbacks = _inputs()
    runtime = module.MarketApplicationRuntime(
        dependencies=dependencies, callbacks=callbacks
    ).create()

    assert runtime.ingestion_components is not None
    assert isinstance(runtime.market_worker_factory, _WorkerFactory)
    assert isinstance(runtime.market_background_services, _BackgroundServices)
    assert runtime.market_worker_factory.kwargs["client_id"] == dependencies["client_id"]
    assert runtime.market_background_services.kwargs["process_market_event"] == runtime.process_market_event


def test_market_application_runtime_preserves_ingestion_callback_behavior(monkeypatch):
    monkeypatch.setattr(module, "MarketIngestionRuntimeFactory", _IngestionFactory)
    monkeypatch.setattr(module, "MarketWorkerFactory", _WorkerFactory)
    monkeypatch.setattr(module, "MarketBackgroundServicesRuntime", _BackgroundServices)
    dependencies, callbacks = _inputs()
    runtime = module.MarketApplicationRuntime(
        dependencies=dependencies, callbacks=callbacks
    ).create()

    assert runtime.reserve_ai_ticker("SBER", "BUY", {"bar_pattern_name": "PULLBACK"})
    assert runtime.ingestion_components.ai_candidate_deduplicator.reserved == [
        ("SBER", "BUY", "PULLBACK")
    ]
    runtime.release_ai_ticker("SBER")
    assert runtime.ingestion_components.ai_candidate_deduplicator.released == ["SBER"]

    runtime.store_pending_bbo_candidate("SBER", {"price": 100.0}, 12.0)
    assert runtime.ingestion_components.pending_bbo_candidate_store.stored == [
        ("SBER", {"price": 100.0}, 12.0)
    ]
    runtime.drop_pending_bbo_candidate("SBER")
    runtime.cancel_pending_bbo_candidate("SBER")
    runtime.expire_pending_bbo_candidates(15.0)
    assert runtime.ingestion_components.pending_bbo_runtime.dropped == [("SBER", None)]
    assert runtime.ingestion_components.pending_bbo_runtime.cancelled == [("SBER", None)]
    assert runtime.ingestion_components.pending_bbo_runtime.expired == [15.0]

    event = {"type": "QUOTES", "ticker": "SBER"}
    asyncio.run(runtime.process_market_event(event))
    assert runtime.ingestion_components.market_data_event_runtime.events == [event]
