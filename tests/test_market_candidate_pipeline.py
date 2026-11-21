from __future__ import annotations
import asyncio
from strategy.market_candidate_pipeline import MarketCandidatePipeline


class Logger:
    def __init__(self): self.errors = []
    def debug(self, *args): pass
    def error(self, *args): self.errors.append(args)


class Queue:
    def __init__(self, full=False): self.items, self.full = [], full
    def put_nowait(self, item):
        if self.full: raise asyncio.QueueFull
        self.items.append(item)


class Engine:
    def __init__(self, active=(), opening=()):
        self.active, self._opening_tickers, self.paper_mode = set(active), set(opening), True
    def get_open_position_tickers(self): return set(self.active)


class Factory:
    def create(self, priority, event_type, payload): return priority, event_type, payload


def make_pipeline(*, evidence=(True, ""), atr=(True, ""), direction="BUY",
                  queue_full=False, reserved=True, conflict="", active=(), opening=(),
                  reason_bucket="other"):
    c = {k: [] for k in ("counters", "shadow", "not_called", "pending", "blocked",
                          "guard", "reserved", "released", "numpy", "payloads")}
    queue, logger = Queue(queue_full), Logger()
    def payload(**kwargs): c["payloads"].append(kwargs); return dict(kwargs)
    def shadow(ticker, side, metrics):
        c["shadow"].append((ticker, side)); return {"candidate_id": "shadow-1"}
    def validate(side, metrics, *, paper_mode):
        c["validate"] = (side, paper_mode); return evidence
    def reserve(ticker, side, metrics):
        c["reserved"].append((ticker, side)); return reserved
    pipeline = MarketCandidatePipeline(
        execution_engine=Engine(active, opening), anomaly_queue=queue,
        queue_item_factory=Factory(), trade_flow_increment=lambda key: c["counters"].append(key),
        infer_direction=lambda metrics: direction,
        classify_direction=lambda side, metrics: "CONFIRMED",
        log_shadow_candidate=shadow, queue_flow_conflict_reason=lambda side, metrics: conflict,
        validate_entry_evidence=validate, paper_mode_provider=lambda: True,
        guard_reason_bucket=lambda reason: reason_bucket,
        store_pending_bbo_candidate=lambda ticker, data, received: c["pending"].append((ticker, data, received)),
        mark_shadow_llm_not_called=lambda cid, reason: c["not_called"].append((cid, reason)),
        trade_flow_guard_block=lambda reason: c["guard"].append(reason),
        log_prefilter_blocked=lambda *a, **kw: c["blocked"].append((a, kw)),
        validate_atr_readiness=lambda metrics: atr, reserve_ai_ticker=reserve,
        build_candidate_payload=payload,
        release_ai_ticker=lambda ticker: c["released"].append(ticker),
        update_numpy_impulse_status=lambda ticker: c["numpy"].append(ticker),
        logger=logger, monotonic=lambda: 123.0,
    )
    return pipeline, c, queue, logger


def candidate(**overrides):
    result = {"is_anomaly": True, "selected": True, "selection_ready": True,
              "metrics": {"price_change_pct": 0.2}}
    result.update(overrides)
    return result


def run(pipeline, analysis=None, **overrides):
    args = dict(ticker="SBER", price=100., volume=10., bid=99.9, ask=100.1,
                bid_size=5., ask_size=6., day_volume=1000., analysis=analysis or candidate(),
                supplied_metrics={}, session_generation=7, received_monotonic=55.)
    args.update(overrides)
    return pipeline.process_candidate(**args)


def test_non_candidate_has_no_side_effects():
    p,c,q,_=make_pipeline()
    assert run(p,candidate(is_anomaly=False)) is False
    assert not c["counters"] and not q.items


def test_open_or_opening_ticker_is_skipped_before_shadow():
    for active, opening in [({"SBER"}, set()), (set(), {"SBER"})]:
        p,c,q,_=make_pipeline(active=active,opening=opening)
        assert run(p) is True
        assert c["counters"] == ["selected_anomalies","active_position_skipped"]
        assert not c["shadow"] and not q.items


def test_missing_direction_stops_before_shadow():
    p,c,q,_=make_pipeline(direction=None)
    assert run(p) is True
    assert c["counters"][-1] == "direction_prefilter_skipped"
    assert len(c["blocked"]) == 1 and not c["shadow"] and not q.items


def test_quote_failure_is_deferred_with_original_context():
    p,c,q,_=make_pipeline(evidence=(False,"Bid/Ask котировки неактуальны"),reason_bucket="quotes")
    assert run(p) is True
    ticker,data,received=c["pending"][0]
    assert ticker=="SBER" and received==55.
    assert data["candidate_direction"]=="BUY" and data["candidate_created_monotonic"]==123.
    assert not c["not_called"] and not q.items


def test_other_evidence_failure_records_shadow_and_guard():
    p,c,q,_=make_pipeline(evidence=(False,"Спред выше лимита"))
    assert run(p) is True
    assert c["not_called"]==[("shadow-1","SIGNAL_EVIDENCE_BLOCKED")]
    assert c["guard"]==["Спред выше лимита"]
    assert c["counters"][-1]=="prefilter_blocked" and not q.items


def test_atr_is_hard_pre_llm_gate():
    p,c,q,_=make_pipeline(atr=(False,"ATR M1-история недостаточна"))
    assert run(p) is True
    assert c["not_called"]==[("shadow-1","ATR_NOT_READY")]
    assert c["blocked"][0][1]=={"interval_sec":60.}
    assert c["counters"][-1]=="atr_unready_candidates" and not q.items


def test_deduplication_prevents_queueing():
    p,c,q,_=make_pipeline(reserved=False)
    assert run(p) is True
    assert c["not_called"]==[("shadow-1","AI_DEDUPLICATED")]
    assert c["counters"][-1]=="candidate_deduplicated" and not q.items


def test_approved_candidate_is_queued_with_generation_and_direction():
    p,c,q,logger=make_pipeline()
    assert run(p) is True
    priority,event,payload=q.items[0]
    assert (priority,event)==(1,"MARKET_TICK")
    assert payload["ticker"]=="SBER" and payload["candidate_direction"]=="BUY"
    assert payload["paper_session_generation"]==7 and payload["candidate_created_monotonic"]==123.
    assert c["counters"][-2:]==["anomalies_queued","prefilter_pass_buy"]
    assert c["numpy"]==["SBER"] and not logger.errors


def test_queue_overflow_releases_reservation():
    p,c,q,logger=make_pipeline(queue_full=True)
    assert run(p) is True and not q.items
    assert c["not_called"]==[("shadow-1","AI_QUEUE_FULL")]
    assert c["released"]==["SBER"] and c["counters"][-1]=="queue_dropped"
    assert len(logger.errors)==1


def test_queue_flow_conflict_blocks_before_entry_evidence_validation():
    p,c,q,_=make_pipeline(conflict="поток против направления")
    assert run(p) is True
    assert "validate" not in c and c["guard"]==["поток против направления"]
    assert not q.items
