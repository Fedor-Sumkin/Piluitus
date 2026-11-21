from __future__ import annotations

import asyncio

from runtime.market_event_router import MarketAuxiliaryEventRouter, PaperBBOReplayAdapter


class Logger:
    def __init__(self): self.messages = []
    def debug(self, *args): self.messages.append(args)


class Recorder:
    def __init__(self): self.rows = []
    def record(self, ticker, state, event):
        self.rows.append((ticker, dict(state), dict(event)))
        return bool(state.get("updated"))


def test_bbo_replay_adapter_records_order_book_and_confirmation_context():
    recorder = Recorder()
    screener = type("S", (), {"quotes": {"SBER": {
        "bid": 100, "ask": 100.1, "bid_size": 7, "ask_size": 9,
        "last_bid_confirmed_seen": 10, "last_ask_confirmed_seen": 10.1,
        "order_book_valid": True,
    }}, "bbo_max_age_sec": 1.5})()
    adapter = PaperBBOReplayAdapter(recorder_provider=lambda: recorder, screener=screener)
    assert adapter.record("sber@tqbr", source="FINAM_ORDER_BOOK",
        event_timestamp="book-ts", received_monotonic=20,
        book_result={"updated": True, "valid": True, "bid": 100, "ask": 100.1,
                     "bid_size": 7, "ask_size": 9})
    ticker, state, event = recorder.rows[0]
    assert ticker == "SBER" and state["updated"] and state["valid"]
    assert state["order_book_valid"] is True
    assert event["order_book_timestamp"] == "book-ts"
    assert event["bid_confirmed_monotonic"] == 10
    assert event["ask_confirmed_monotonic"] == 10.1
    assert event["max_age_sec"] == 1.5


def test_bbo_replay_adapter_records_one_sided_quote_separately_from_book_validity():
    recorder = Recorder()
    screener = type("S", (), {"quotes": {"GAZP": {
        "bid": 90, "ask": 0, "bid_size": 11, "ask_size": 0,
        "last_bid_confirmed_seen": 15, "last_ask_confirmed_seen": 0,
        "order_book_valid": False, "order_book_last_reason": "NO_ORDER_BOOK",
    }}, "bbo_max_age_sec": 2.0})()
    adapter = PaperBBOReplayAdapter(recorder_provider=lambda: recorder, screener=screener)
    assert adapter.record("GAZP", source="FINAM_QUOTES",
        event_timestamp="quote-ts", received_monotonic=15, sides_present=True)
    ticker, state, event = recorder.rows[0]
    assert ticker == "GAZP"
    assert state["valid"] is True and state["order_book_valid"] is False
    assert state["bid"] == 90 and state["ask"] == 0
    assert event["source"] == "FINAM_QUOTES"
    assert event["timestamp"] == "quote-ts"
    assert "order_book_timestamp" not in event


def test_bbo_replay_adapter_skips_quote_packet_without_accepted_sides():
    recorder = Recorder()
    adapter = PaperBBOReplayAdapter(
        recorder_provider=lambda: recorder,
        screener=type("S", (), {"quotes": {"SBER": {"bid": 100, "ask": 101}}})())
    assert adapter.record("SBER", source="FINAM_QUOTES", event_timestamp=1,
        received_monotonic=1, sides_present=False) is False
    assert len(recorder.rows) == 1 and recorder.rows[0][1]["updated"] is False


class FakeScreener:
    def __init__(self, book_result=None, trade_result=None):
        self.book_result = book_result or {
            "updated": True, "valid": True, "bbo_side_touch_count": 2,
            "heartbeat_only": False, "bid": 100, "ask": 100.1,
            "bid_size": 10, "ask_size": 12}
        self.trade_result = trade_result or {"accepted": True}
        self.book_calls, self.trade_calls = [], []
    async def push_order_book(self, **kwargs):
        self.book_calls.append(kwargs)
        return dict(self.book_result)
    def push_trade(self, **kwargs):
        self.trade_calls.append(kwargs)
        return dict(self.trade_result)


class FakeShadow:
    def __init__(self): self.observations = []
    def observe_market_tick(self, ticker, metrics):
        self.observations.append((ticker, dict(metrics)))
        return [{"candidate_id": "shadow-1"}]


class FakeReplay:
    def __init__(self): self.calls = []
    def record(self, *args, **kwargs): self.calls.append((args, kwargs)); return True


def make_router(*, book_result=None, trade_result=None, paper_mode=False):
    screener = FakeScreener(book_result, trade_result)
    engine = type("E", (), {})()
    engine.paper_mode = paper_mode
    engine.paper_positions = {"SBER": {"id": "PAPER-1"}}
    engine.scheduled = []
    engine._latest_price = lambda ticker, position: 99.5
    engine.schedule_paper_position_monitor = lambda *a, **k: engine.scheduled.append((a, k))
    counters, diagnostics, persisted, resumed = [], [], [], []
    logger, shadow, replay = Logger(), FakeShadow(), FakeReplay()
    async def persist(labels, *, source): persisted.append((labels, source))
    async def resume(ticker, *, trigger): resumed.append((ticker, trigger))
    def refresh(received_screener, ticker, metrics, *, now):
        assert received_screener is screener
        metrics["bid"] = 100
        return True, "fresh"
    router = MarketAuxiliaryEventRouter(
        screener=screener, execution_engine=engine, db_manager="db", logger=logger,
        trade_flow_increment=lambda key, amount: counters.append((key, amount)),
        record_position_order_book_event=lambda ticker, result: diagnostics.append((ticker, result)),
        bbo_replay=replay, refresh_bbo_metrics=refresh, shadow_tracker=shadow,
        persist_shadow_outcomes=persist, resume_pending_bbo_candidate=resume,
        monotonic=lambda: 50)
    return type("State", (), dict(router=router, screener=screener, engine=engine,
        counters=counters, diagnostics=diagnostics, persisted=persisted, resumed=resumed,
        logger=logger, shadow=shadow, replay=replay))()


def test_trade_router_normalizes_and_records_accepted_print():
    s = make_router()
    assert asyncio.run(s.router.process_if_handled({
        "event_type": "instrument_trade", "ticker": " sber@tqbr ",
        "price": 100.2, "size": 4, "trade_id": "T-1", "side": "BUY",
        "timestamp": "trade-ts", "received_monotonic": 10}))
    assert s.screener.trade_calls[0]["ticker"] == "SBER"
    assert ("instrument_trades_received", 1) in s.counters


def test_trade_router_classifies_rejected_and_missing_ticker_prints():
    rejected = make_router(trade_result={"accepted": False, "reason": "duplicate"})
    asyncio.run(rejected.router.process_if_handled({"event_type": "INSTRUMENT_TRADE", "ticker": "SBER"}))
    assert ("instrument_trades_duplicate", 1) in rejected.counters
    missing = make_router()
    asyncio.run(missing.router.process_if_handled({"event_type": "INSTRUMENT_TRADE", "ticker": ""}))
    assert ("instrument_trades_invalid", 1) in missing.counters
    assert not missing.screener.trade_calls


def test_router_leaves_quote_and_unrecognized_events_for_quote_pipeline():
    s = make_router()
    assert asyncio.run(s.router.process_if_handled({"ticker": "SBER", "price": 100})) is False
    assert asyncio.run(s.router.process_if_handled({"event_type": "NEWS"})) is False
    assert not s.screener.book_calls and not s.screener.trade_calls


def test_order_book_router_updates_diagnostics_replay_shadow_and_paper_exit_monitor():
    s = make_router(paper_mode=True)
    assert asyncio.run(s.router.process_if_handled({
        "event_type": "ORDER_BOOK", "ticker": "sber@TQBR",
        "bid": 100, "ask": 100.1, "bid_size": 10, "ask_size": 12,
        "order_book_timestamp": "book-ts", "received_monotonic": 50}))
    assert s.screener.book_calls[0]["ticker"] == "SBER"
    assert s.screener.book_calls[0]["bid_timestamp"] == "book-ts"
    assert s.diagnostics[0][0] == "SBER"
    assert s.replay.calls[0][0][0] == "SBER"
    assert ("orderbook_updates", 1) in s.counters
    assert ("orderbook_side_touches", 2) in s.counters
    assert s.shadow.observations[0][0] == "SBER"
    assert s.persisted == [([{"candidate_id": "shadow-1"}], "ORDER_BOOK")]
    assert s.resumed == [("SBER", "ORDER_BOOK")]
    assert len(s.engine.scheduled) == 1
    args, kwargs = s.engine.scheduled[0]
    assert args[0] == "SBER" and args[1] == 99.5
    assert kwargs["event_source"] == "ORDER_BOOK"
    assert kwargs["event_timestamp"] == "book-ts"


def test_order_book_router_classifies_invalid_and_out_of_order_states():
    invalid = make_router(book_result={"updated": True, "valid": False, "reason": "CROSSED_BBO"})
    asyncio.run(invalid.router.process_if_handled({"event_type": "ORDER_BOOK", "ticker": "SBER"}))
    assert ("orderbook_invalid", 1) in invalid.counters and invalid.logger.messages
    stale = make_router(book_result={"updated": False, "valid": False, "out_of_order": True, "reason": "stale"})
    asyncio.run(stale.router.process_if_handled({"event_type": "ORDER_BOOK", "ticker": "SBER"}))
    assert ("orderbook_out_of_order", 1) in stale.counters


def test_order_book_router_rejects_malformed_payload_and_logs_unusable_state():
    malformed = make_router()
    asyncio.run(malformed.router.process_if_handled({
        "event_type": "ORDER_BOOK", "ticker": "SBER", "bid": "not-number"}))
    assert ("orderbook_unusable", 1) in malformed.counters and not malformed.screener.book_calls
    unusable = make_router(book_result={"updated": False, "reason": "not accepted"})
    asyncio.run(unusable.router.process_if_handled({"event_type": "ORDER_BOOK", "ticker": "SBER"}))
    assert ("orderbook_unusable", 1) in unusable.counters and unusable.logger.messages
