from __future__ import annotations

import asyncio

import pytest

from strategy.market_data_runtime import (
    PendingBBOCandidateRuntime,
    PendingBBOCandidateStore,
)


class Logger:
    def __init__(self):
        self.debug_messages = []
        self.info_messages = []
        self.error_messages = []

    def debug(self, *args):
        self.debug_messages.append(args)

    def info(self, *args):
        self.info_messages.append(args)

    def error(self, *args):
        self.error_messages.append(args)


class Queue:
    def __init__(self, *, full=False):
        self.full = full
        self.items = []

    def put_nowait(self, item):
        if self.full:
            raise asyncio.QueueFull
        self.items.append(item)


class QueueItemFactory:
    def create(self, priority, event_type, payload):
        return priority, event_type, payload


class Engine:
    def __init__(self):
        self.paper_mode = True
        self._maintenance_active = False
        self._opening_tickers = set()
        self.active_tickers = set()

    def get_open_position_tickers(self):
        return set(self.active_tickers)


class Screener:
    def __init__(self):
        self.status = {"ready": True, "selected_tickers": ["SBER"]}

    def get_status(self):
        return dict(self.status)


def make_runtime(
    *,
    now=100.5,
    created_at=100.0,
    payload=None,
    queue_full=False,
    refresh=(True, "fresh BBO"),
    direction="BUY",
    evidence=(True, ""),
    atr=(True, ""),
    conflict="",
    reserved=True,
):
    counters = []
    not_called = []
    guard_blocks = []
    blocked_logs = []
    shadow_logs = []
    numpy_updates = []
    released = []
    queue = Queue(full=queue_full)
    logger = Logger()
    engine = Engine()
    screener = Screener()
    metrics = {"bid": 100.0, "ask": 100.1, "bid_size": 10.0, "ask_size": 12.0}
    candidate_payload = payload if payload is not None else {
        "ticker": "SBER",
        "candidate_direction": "BUY",
        "paper_session_generation": 7,
        "shadow_candidate_id": "shadow-1",
        "metrics": metrics,
    }
    store = PendingBBOCandidateStore(
        ttl_sec=1.5,
        trade_flow_increment=lambda key, amount=1: counters.append((key, amount)),
        logger=logger,
        monotonic=lambda: now,
    )
    store.candidates["SBER"] = {
        "created_at": created_at,
        "payload": candidate_payload,
    }
    callbacks = {
        "refresh": lambda received_screener, ticker, received_metrics, *, now: (
            refresh[0],
            refresh[1],
        ),
        "infer": lambda received_metrics: direction,
        "evidence": lambda side, received_metrics, *, paper_mode: evidence,
        "atr": lambda received_metrics: atr,
        "conflict": lambda side, received_metrics: conflict,
        "reserve": lambda ticker, side, received_metrics: reserved,
    }
    if refresh[0]:
        def refresh_bbo(received_screener, ticker, received_metrics, *, now):
            received_metrics.update({
                "bid": 101.0,
                "ask": 101.1,
                "bid_size": 9.0,
                "ask_size": 8.0,
            })
            return refresh
        callbacks["refresh"] = refresh_bbo

    runtime = PendingBBOCandidateRuntime(
        store=store,
        screener=screener,
        execution_engine=engine,
        anomaly_queue=queue,
        queue_item_factory=QueueItemFactory(),
        refresh_bbo_metrics=callbacks["refresh"],
        infer_candidate_direction=callbacks["infer"],
        paper_mode_provider=lambda: engine.paper_mode,
        validate_pre_llm_entry_evidence=callbacks["evidence"],
        validate_atr_readiness=callbacks["atr"],
        queue_flow_conflict_reason=callbacks["conflict"],
        reserve_ai_ticker=lambda ticker, side, received_metrics: (
            released.append(("reserved", ticker)) or callbacks["reserve"](
                ticker, side, received_metrics
            )
        ),
        release_ai_ticker=lambda ticker: released.append(("released", ticker)),
        mark_shadow_llm_not_called=lambda candidate_id, reason: not_called.append(
            (candidate_id, reason)
        ),
        log_shadow_candidate=lambda ticker, side, received_metrics: (
            shadow_logs.append((ticker, side)) or {"candidate_id": "shadow-new"}
        ),
        trade_flow_increment=lambda key: counters.append((key, 1)),
        guard_reason_bucket=lambda reason: (
            "quotes" if "quote" in str(reason).lower() else "other"
        ),
        trade_flow_guard_block=lambda reason: guard_blocks.append(reason),
        log_prefilter_blocked=lambda *args, **kwargs: blocked_logs.append(
            (args, kwargs)
        ),
        update_numpy_impulse_status=lambda ticker: numpy_updates.append(ticker),
        session_generation_provider=lambda: 7,
        logger=logger,
        monotonic=lambda: now,
    )
    return {
        "runtime": runtime,
        "store": store,
        "queue": queue,
        "engine": engine,
        "screener": screener,
        "counters": counters,
        "not_called": not_called,
        "guard_blocks": guard_blocks,
        "blocked_logs": blocked_logs,
        "shadow_logs": shadow_logs,
        "numpy_updates": numpy_updates,
        "released": released,
        "logger": logger,
        "payload": candidate_payload,
    }


def counter_keys(state):
    return [key for key, _ in state["counters"]]


def resume(state, ticker="SBER", trigger="ORDER_BOOK"):
    return asyncio.run(state["runtime"].resume(ticker, trigger=trigger))


def test_fresh_book_candidate_is_revalidated_and_enqueued_once():
    state = make_runtime()

    resume(state, ticker=" sber@TQBR ", trigger="ORDER_BOOK")

    assert len(state["queue"].items) == 1
    priority, event_type, payload = state["queue"].items[0]
    assert (priority, event_type) == (1, "MARKET_TICK")
    assert payload is state["payload"]
    assert payload["bid"] == 101.0
    assert payload["ask"] == 101.1
    assert payload["bid_size"] == 9.0
    assert payload["ask_size"] == 8.0
    assert state["store"].count() == 0
    assert state["numpy_updates"] == ["SBER"]
    assert "bbo_resume_attempted" in counter_keys(state)
    assert "bbo_resume_from_book" in counter_keys(state)
    assert "bbo_resumed" in counter_keys(state)
    assert "prefilter_pass_buy" in counter_keys(state)


def test_fresh_quote_can_resume_only_a_still_fresh_candidate():
    state = make_runtime()

    resume(state, trigger="QUOTES")

    assert len(state["queue"].items) == 1
    assert "bbo_resume_from_quotes" in counter_keys(state)


def test_expired_candidate_is_dropped_without_entering_the_ai_queue():
    state = make_runtime(created_at=98.0)

    resume(state)

    assert state["queue"].items == []
    assert state["store"].count() == 0
    assert "bbo_resume_expired" in counter_keys(state)
    assert not state["not_called"]


def test_invalid_payload_is_cancelled_and_recorded():
    state = make_runtime(payload="not-a-dict")

    resume(state)

    assert state["store"].count() == 0
    assert state["not_called"] == []
    assert "bbo_resume_invalid_payload" in counter_keys(state)
    assert "bbo_cancelled" in counter_keys(state)


@pytest.mark.parametrize(
    ("maintenance", "generation", "reason"),
    [
        (True, 7, "PAPER_SESSION_CHANGED"),
        (False, 6, "PAPER_SESSION_CHANGED"),
    ],
)
def test_session_change_or_maintenance_cancels_candidate(
    maintenance, generation, reason
):
    state = make_runtime()
    state["engine"]._maintenance_active = maintenance
    state["payload"]["paper_session_generation"] = generation

    resume(state)

    assert state["store"].count() == 0
    assert state["not_called"] == [("shadow-1", reason)]
    assert "bbo_resume_session_cancelled" in counter_keys(state)


def test_unready_funnel_cancels_candidate():
    state = make_runtime()
    state["screener"].status["ready"] = False

    resume(state)

    assert state["not_called"] == [("shadow-1", "FUNNEL_NOT_READY")]
    assert state["store"].count() == 0


def test_top15_deselection_cancels_candidate():
    state = make_runtime()
    state["screener"].status["selected_tickers"] = ["GAZP"]

    resume(state)

    assert state["not_called"] == [("shadow-1", "TOP15_DESELECTED")]
    assert state["store"].count() == 0


def test_active_or_opening_position_cancels_candidate_before_refresh():
    for active, opening in [({"SBER"}, set()), (set(), {"SBER"})]:
        state = make_runtime()
        state["engine"].active_tickers = active
        state["engine"]._opening_tickers = opening

        resume(state)

        assert state["not_called"] == [("shadow-1", "POSITION_ALREADY_OPEN")]
        assert state["store"].count() == 0
        assert "bbo_resume_active_position" in counter_keys(state)


def test_invalid_metrics_cancel_candidate():
    state = make_runtime()
    state["payload"]["metrics"] = None

    resume(state)

    assert state["not_called"] == [("shadow-1", "INVALID_METRICS_PAYLOAD")]
    assert state["store"].count() == 0


def test_stale_or_incoherent_book_keeps_candidate_waiting_until_ttl():
    state = make_runtime(refresh=(False, "ORDER_BOOK stale"))

    resume(state)

    assert state["store"].count() == 1
    assert state["queue"].items == []
    assert "bbo_resume_other" in counter_keys(state)
    assert state["logger"].debug_messages


def test_direction_change_cancels_candidate():
    state = make_runtime(direction="SELL")

    resume(state)

    assert state["not_called"] == [("shadow-1", "DIRECTION_CHANGED")]
    assert state["store"].count() == 0
    assert "direction_prefilter_skipped" in counter_keys(state)


def test_queue_flow_conflict_is_deferred_and_candidate_reaches_llm_queue():
    state = make_runtime(conflict="queue/flow contradiction")

    resume(state)

    assert state["queue"].items
    assert state["not_called"] == []
    assert state["guard_blocks"] == []
    assert state["store"].count() == 0


def test_transient_quote_evidence_failure_keeps_candidate_waiting():
    state = make_runtime(evidence=(False, "quote confirmation not fresh"))

    resume(state)

    assert state["store"].count() == 1
    assert state["queue"].items == []
    assert not state["not_called"]
    assert "bbo_resume_evidence_quotes" in counter_keys(state)


def test_non_quote_evidence_failure_cancels_candidate():
    state = make_runtime(evidence=(False, "candidate metrics malformed"))

    resume(state)

    assert state["not_called"] == [("shadow-1", "SIGNAL_EVIDENCE_BLOCKED")]
    assert state["store"].count() == 0
    assert "prefilter_blocked" in counter_keys(state)
    assert state["guard_blocks"] == ["spread above local limit"]


def test_atr_readiness_remains_a_hard_pre_llm_gate():
    state = make_runtime(atr=(False, "ATR M1 history not ready"))

    resume(state)

    assert state["not_called"] == [("shadow-1", "ATR_NOT_READY")]
    assert state["store"].count() == 0
    assert "bbo_resume_atr_blocked" in counter_keys(state)
    assert state["blocked_logs"][0][1] == {"interval_sec": 60.0}


def test_superseded_payload_is_not_enqueued_or_removed():
    state = make_runtime()
    old_pending = state["store"].get("SBER")
    newer_payload = {
        "ticker": "SBER",
        "candidate_direction": "BUY",
        "paper_session_generation": 7,
        "shadow_candidate_id": "shadow-newer",
        "metrics": {"bid": 102.0, "ask": 102.1},
    }

    def supersede(received_screener, ticker, metrics, *, now):
        state["store"].candidates["SBER"] = {
            "created_at": 100.4,
            "payload": newer_payload,
        }
        return True, "fresh"
    state["runtime"].refresh_bbo_metrics = supersede

    resume(state)

    assert state["store"].get("SBER") is not old_pending
    assert state["store"].get("SBER")["payload"] is newer_payload
    assert state["queue"].items == []
    assert state["not_called"] == [("shadow-1", "PAYLOAD_SUPERSEDED")]


def test_deduplication_blocks_candidate_and_removes_pending_entry():
    state = make_runtime(reserved=False)

    resume(state)

    assert state["not_called"] == [("shadow-1", "AI_DEDUPLICATED")]
    assert state["store"].count() == 0
    assert "candidate_deduplicated" in counter_keys(state)


def test_ai_queue_overflow_releases_reservation_and_discards_pending_entry():
    state = make_runtime(queue_full=True)

    resume(state)

    assert state["queue"].items == []
    assert state["released"][-1] == ("released", "SBER")
    assert state["store"].count() == 0
    assert state["not_called"] == [("shadow-1", "AI_QUEUE_FULL")]
    assert "bbo_resume_queue_full" in counter_keys(state)
    assert state["logger"].error_messages


def test_missing_shadow_id_is_created_only_after_fresh_book_and_direction():
    state = make_runtime()
    state["payload"]["shadow_candidate_id"] = None

    resume(state)

    assert state["shadow_logs"] == [("SBER", "BUY")]
    assert state["queue"].items[0][2]["shadow_candidate_id"] == "shadow-new"


def test_store_lifecycle_facade_preserves_ttl_and_identity_semantics():
    state = make_runtime()
    runtime = state["runtime"]
    runtime.drop("SBER")
    assert not runtime.has_pending("SBER")

    runtime.store_candidate(
        "gazp@tqbr",
        {
            "candidate_direction": "SELL",
            "metrics": {"bid": 90.0, "ask": 90.1},
        },
        received_monotonic=99.0,
    )
    assert runtime.has_pending("GAZP")
    assert runtime.store.count() == 1
    assert runtime.cancel("GAZP")
    assert not runtime.has_pending("GAZP")
