from __future__ import annotations

import asyncio
from types import SimpleNamespace

from runtime.market_data_event_runtime import MarketDataEventRuntime


class Logger:
    def __init__(self):
        self.info_messages, self.debug_messages, self.error_messages = [], [], []
    def info(self, *args): self.info_messages.append(args)
    def debug(self, *args): self.debug_messages.append(args)
    def error(self, *args, **kwargs): self.error_messages.append((args, kwargs))


class Terminal:
    def __init__(self): self.messages, self.funnel = [], []
    def update(self, message): self.messages.append(message)
    def update_funnel(self, message): self.funnel.append(message)


class Screener:
    def __init__(self, *, analysis=None, status=None, error=None):
        self.analysis = analysis or {
            "is_anomaly": True, "selected": True, "selection_ready": True,
            "metrics": {"funnel_rank": 1, "funnel_score": 12.5},
            "quote_side_updates": {"bid": True, "ask": True},
        }
        self.status = status or {
            "ready": True, "selected_tickers": ["SBER", "GAZP"],
            "universe_count": 88, "observed_count": 88,
            "eligible_count": 30, "top_n": 15,
        }
        self.error, self.push_calls = error, []
    async def push_tick(self, **kwargs):
        if self.error: raise self.error
        self.push_calls.append(kwargs)
        return self.analysis
    def get_status(self): return dict(self.status)
    def get_status_text(self): return "Top-15 ready"


class AuxiliaryRouter:
    def __init__(self, *, handled=False):
        self.handled, self.events, self.replay_calls = handled, [], []
        self.bbo_replay = SimpleNamespace(record=self.record)
    async def process_if_handled(self, event):
        self.events.append(event)
        return self.handled
    def record(self, *args, **kwargs): self.replay_calls.append((args, kwargs))


class PendingBBO:
    def __init__(self, *, pending=False):
        self.pending, self.expirations, self.checks, self.resumes = pending, 0, [], []
    def expire(self): self.expirations += 1
    def has_pending(self, ticker):
        self.checks.append(ticker)
        return self.pending
    async def resume(self, ticker, *, trigger):
        self.resumes.append((ticker, trigger))


class ShadowTracker:
    def __init__(self, labels=None): self.labels, self.observations = list(labels or []), []
    def observe_market_tick(self, ticker, metrics):
        self.observations.append((ticker, dict(metrics)))
        return list(self.labels)


class CandidatePipeline:
    def __init__(self): self.calls = []
    def process_candidate(self, **kwargs): self.calls.append(kwargs)


class ExecutionEngine:
    def __init__(self):
        self._maintenance_active, self.monitor_calls = False, []
    def schedule_paper_position_monitor(self, *args, **kwargs):
        self.monitor_calls.append((args, kwargs))


def make_runtime(
    *, handled=False, pending=False, analysis=None, status=None,
    shadow_labels=None, maintenance=False, screener_error=None,
):
    counters, warmups, persisted = [], [], []
    engine = ExecutionEngine()
    engine._maintenance_active = maintenance
    screener = Screener(analysis=analysis, status=status, error=screener_error)
    auxiliary, pending_bbo = AuxiliaryRouter(handled=handled), PendingBBO(pending=pending)
    shadow, candidates, terminal, logger = (
        ShadowTracker(shadow_labels), CandidatePipeline(), Terminal(), Logger()
    )
    clock = [10.0]
    async def persist_shadow_outcomes(labels, *, source):
        persisted.append((labels, source))

    runtime = MarketDataEventRuntime(
        execution_engine=engine,
        db_manager="db",
        screener=screener,
        auxiliary_event_router=auxiliary,
        candidate_pipeline=candidates,
        pending_bbo_runtime=pending_bbo,
        shadow_tracker=shadow,
        persist_shadow_outcomes=persist_shadow_outcomes,
        schedule_m1_history_warmup=lambda tickers: warmups.append(tuple(tickers)),
        trade_flow_increment=lambda key: counters.append(key),
        terminal_status=terminal,
        logger=logger,
        session_generation_provider=lambda: 17,
        monotonic=lambda: clock[0],
        spinner_frames=("|", "/", "-", "\\"),
    )
    return SimpleNamespace(
        runtime=runtime, engine=engine, screener=screener, auxiliary=auxiliary,
        pending=pending_bbo, shadow=shadow, candidates=candidates, terminal=terminal,
        logger=logger, counters=counters, warmups=warmups, persisted=persisted, clock=clock,
    )


def quote_event(**overrides):
    event = {
        "ticker": "sber@tqbr", "price": 100.5, "volume": 12,
        "bid": 100.4, "ask": 100.6, "bid_size": 5, "ask_size": 7,
        "day_volume": 1000, "timestamp": "quote-ts",
        "received_monotonic": 9.5, "metrics": {"supplied": True},
    }
    event.update(overrides)
    return event


def process(state, event=None):
    return asyncio.run(state.runtime.process_event(event or quote_event()))


def test_quote_event_flows_through_replay_shadow_funnel_pipeline_and_position_monitor():
    state = make_runtime(shadow_labels=[{"candidate_id": "label-1"}])
    process(state)
    assert state.pending.expirations == 1
    assert len(state.screener.push_calls) == 1
    assert state.screener.push_calls[0] == {
        "ticker": "SBER", "price": 100.5, "volume": 12.0,
        "bid": 100.4, "ask": 100.6, "bid_size": 5.0, "ask_size": 7.0,
        "day_volume": 1000.0, "bid_present": True, "ask_present": True,
        "bid_size_present": True, "ask_size_present": True,
        "event_timestamp": "quote-ts", "received_monotonic": 9.5,
    }
    args, kwargs = state.auxiliary.replay_calls[0]
    assert args == ("SBER",)
    assert kwargs == {
        "source": "FINAM_QUOTES", "event_timestamp": "quote-ts",
        "received_monotonic": 9.5, "sides_present": True,
    }
    assert state.shadow.observations == [
        ("SBER", {"funnel_rank": 1, "funnel_score": 12.5})
    ]
    assert state.persisted == [([{"candidate_id": "label-1"}], "QUOTES")]
    assert state.warmups == [("SBER", "GAZP")]
    assert state.counters == ["quotes_received", "selected_quotes"]
    assert state.candidates.calls[0]["ticker"] == "SBER"
    assert state.candidates.calls[0]["session_generation"] == 17
    assert state.candidates.calls[0]["received_monotonic"] == 9.5
    assert state.engine.monitor_calls[0][0] == ("SBER", 100.5, "db")
    assert state.engine.monitor_calls[0][1] == {
        "event_source": "LAST_TRADE", "event_timestamp": "quote-ts"
    }
    assert state.terminal.messages[0].endswith("Тиков: 1 | SBER 100.5₽")
    assert state.terminal.funnel == ["Top-15 ready"]
    assert state.logger.info_messages[0][0].startswith("🎯 ВОРОНКА ГОТОВА:")


def test_auxiliary_order_book_event_is_routed_and_not_treated_as_a_quote():
    state = make_runtime(handled=True)
    event = {"event_type": "ORDER_BOOK", "ticker": "SBER"}
    process(state, event)
    assert state.pending.expirations == 1
    assert state.auxiliary.events == [event]
    assert state.screener.push_calls == [] and state.candidates.calls == []
    assert state.engine.monitor_calls == [] and state.terminal.messages == []


def test_maintenance_gate_returns_before_expiration_and_event_routing():
    state = make_runtime(maintenance=True)
    process(state)
    assert state.pending.expirations == 0
    assert state.auxiliary.events == [] and state.screener.push_calls == []


def test_invalid_price_or_negative_volume_is_ignored_after_routing():
    for event in (quote_event(price=0), quote_event(volume=-1)):
        state = make_runtime()
        process(state, event)
        assert state.pending.expirations == 1
        assert state.auxiliary.events == [event]
        assert state.screener.push_calls == [] and state.candidates.calls == []


def test_pending_candidate_is_resumed_by_quote_stream_only_when_present():
    state = make_runtime(pending=True)
    process(state)
    assert state.pending.checks == ["SBER"]
    assert state.pending.resumes == [("SBER", "QUOTES")]
    state = make_runtime(pending=False)
    process(state)
    assert state.pending.checks == ["SBER"] and state.pending.resumes == []


def test_one_sided_unaccepted_quote_is_not_recorded_as_bbo_replay_confirmation():
    state = make_runtime(analysis={
        "is_anomaly": False, "selected": False, "selection_ready": False,
        "metrics": {}, "quote_side_updates": {"bid": False, "ask": False},
    })
    process(state, quote_event(bid_present=True, ask_present=False))
    assert state.auxiliary.replay_calls[0][1]["sides_present"] is False
    assert state.counters == ["quotes_received"]
    assert state.warmups == [("SBER", "GAZP")]
    assert len(state.candidates.calls) == 1


def test_attribute_based_quote_packets_keep_normalizer_compatibility():
    state = make_runtime()
    event = SimpleNamespace(
        ticker="GAZP@TQBR", price=200, volume=2, bid=199.9, ask=200.1,
        bid_size=10, ask_size=12, day_volume=500, timestamp="object-ts",
        received_monotonic=20.0, metrics={"source": "object"},
    )
    process(state, event)
    assert state.screener.push_calls[0]["ticker"] == "GAZP"
    assert state.screener.push_calls[0]["event_timestamp"] == "object-ts"
    assert state.candidates.calls[0]["supplied_metrics"] == {"source": "object"}


def test_funnel_warmup_log_is_rate_limited_and_selection_log_tracks_transition():
    state = make_runtime(status={
        "ready": False, "selected_tickers": [], "universe_count": 88,
        "observed_count": 44, "eligible_count": 9, "top_n": 15,
    })
    process(state)
    first_log_count = len(state.logger.info_messages)
    process(state)
    assert len(state.logger.info_messages) == first_log_count
    assert not state.runtime.last_funnel_ready
    state.screener.status = {
        "ready": True, "selected_tickers": ["SBER"], "universe_count": 88,
        "observed_count": 88, "eligible_count": 30, "top_n": 15,
    }
    process(state)
    assert state.runtime.last_funnel_ready
    assert state.runtime.last_funnel_log_key == ("SBER",)
    assert any("ВОРОНКА ГОТОВА" in str(msg[0]) for msg in state.logger.info_messages)


def test_funnel_log_records_changed_selection_only_after_cooldown():
    state = make_runtime()
    process(state)
    state.screener.status["selected_tickers"] = ["GAZP"]
    state.clock[0] = 20.0
    process(state)
    assert state.runtime.last_funnel_log_key == ("SBER", "GAZP")
    state.clock[0] = 41.0
    process(state)
    assert state.runtime.last_funnel_log_key == ("GAZP",)


def test_screener_exception_is_logged_without_escaping_event_callback():
    state = make_runtime(screener_error=RuntimeError("stream failed"))
    process(state)
    assert len(state.logger.error_messages) == 1
    args, kwargs = state.logger.error_messages[0]
    assert "Ошибка в обработчике тиков" in args[0]
    assert kwargs["exc_info"] is True
    assert state.candidates.calls == [] and state.engine.monitor_calls == []


def test_shadow_labels_are_not_persisted_when_tracker_returns_no_new_labels():
    state = make_runtime(shadow_labels=[])
    process(state)
    assert state.shadow.observations and state.persisted == []


def test_funnel_not_ready_still_passes_quote_to_candidate_pipeline_without_warmup():
    state = make_runtime(status={
        "ready": False, "selected_tickers": ["SBER"], "universe_count": 88,
        "observed_count": 12, "eligible_count": 4, "top_n": 15,
    })
    process(state)
    assert state.warmups == []
    assert len(state.candidates.calls) == 1


def test_local_selection_flags_are_still_forwarded_to_candidate_pipeline():
    state = make_runtime(analysis={
        "is_anomaly": False, "selected": False, "selection_ready": False,
        "metrics": {}, "quote_side_updates": {},
    })
    process(state)
    assert state.candidates.calls[0]["analysis"]["is_anomaly"] is False
    assert state.candidates.calls[0]["analysis"]["selected"] is False
