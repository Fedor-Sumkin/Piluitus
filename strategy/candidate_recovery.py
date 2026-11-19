"""Safe refresh of a deferred candidate after a coherent Finam ORDER_BOOK event.

The helper refreshes only quote-derived fields. It deliberately leaves the
original trade price, trade size, VWAP and impulse evidence untouched.
"""
from __future__ import annotations

import math
import time
from collections.abc import Mapping
from typing import Any


def _finite(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, str):
        raw = value
    elif isinstance(value, (int, float)):
        raw = value
    else:
        return default
    try:
        number = float(raw)
    except (TypeError, ValueError, OverflowError):
        return default
    return number if math.isfinite(number) else default


def _refresh_locked(
    screener: Any,
    ticker: str,
    metrics: dict[str, Any],
    now: float,
) -> tuple[bool, str]:
    quotes = getattr(screener, "quotes", None)
    if not isinstance(quotes, Mapping):
        return False, "Скринер не предоставил состояние котировок."

    quote = quotes.get(ticker)
    if not isinstance(quote, Mapping):
        return False, "Для кандидата ещё нет состояния стакана."
    if not bool(quote.get("order_book_valid", False)):
        return False, "ORDER_BOOK пока не подтверждён как целостный."

    bid = _finite(quote.get("bid"))
    ask = _finite(quote.get("ask"))
    if bid <= 0.0 or ask <= 0.0 or ask < bid:
        return False, "В стакане отсутствует двухсторонний непротиворечивый BBO."

    bid_seen = _finite(
        quote.get("last_bid_confirmed_seen", quote.get("last_bid_seen", 0.0))
    )
    ask_seen = _finite(
        quote.get("last_ask_confirmed_seen", quote.get("last_ask_seen", 0.0))
    )
    bid_age = now - bid_seen if bid_seen > 0.0 else math.inf
    ask_age = now - ask_seen if ask_seen > 0.0 else math.inf
    max_age = _finite(getattr(screener, "bbo_max_age_sec", 2.0), 2.0)
    max_age = min(5.0, max(0.25, max_age))

    # A fresh QUOTES BBO is not sufficient if the reconstructed ORDER_BOOK
    # itself is old. Require both a recent coherent book and fresh two-sided
    # confirmations before allowing a deferred candidate to resume.
    last_order_book_seen = _finite(quote.get("last_order_book_seen"))
    order_book_age = now - last_order_book_seen if last_order_book_seen > 0.0 else math.inf
    if (
        not math.isfinite(order_book_age)
        or order_book_age < 0.0
        or order_book_age > max_age
    ):
        return False, "Последнее состояние ORDER_BOOK устарело или не имеет подтверждённого времени."

    if (
        not math.isfinite(bid_age)
        or not math.isfinite(ask_age)
        or bid_age < 0.0
        or ask_age < 0.0
        or bid_age > max_age
        or ask_age > max_age
    ):
        return False, "После ORDER_BOOK Bid/Ask всё ещё не имеют свежего подтверждения."

    mid = (bid + ask) / 2.0
    if mid <= 0.0:
        return False, "Невозможно рассчитать спред по текущему BBO."
    spread_pct = (ask - bid) / mid * 100.0
    bbo_age = max(bid_age, ask_age)

    # Reuse the screener's TTL-aware best-size helpers when available. The
    # full depth is not treated as executable quantity or as proof of order flow.
    bid_size = _finite(quote.get("bid_size"))
    ask_size = _finite(quote.get("ask_size"))
    size_helper = getattr(screener, "_current_top_size", None)
    drop_helper = getattr(screener, "_current_size_drop", None)
    if callable(size_helper):
        bid_size = _finite(size_helper(dict(quote), "bid", now))
        ask_size = _finite(size_helper(dict(quote), "ask", now))
    bid_drop = _finite(quote.get("bid_size_drop_pct"))
    ask_drop = _finite(quote.get("ask_size_drop_pct"))
    if callable(drop_helper):
        bid_drop = _finite(drop_helper(dict(quote), "bid", now))
        ask_drop = _finite(drop_helper(dict(quote), "ask", now))

    bid_size_seen = _finite(quote.get("last_bid_size_seen"))
    ask_size_seen = _finite(quote.get("last_ask_size_seen"))
    bid_size_age = now - bid_size_seen if bid_size_seen > 0.0 else math.inf
    ask_size_age = now - ask_size_seen if ask_size_seen > 0.0 else math.inf
    bid_drop_at = _finite(quote.get("bid_size_drop_at"))
    ask_drop_at = _finite(quote.get("ask_size_drop_at"))
    bid_drop_age = now - bid_drop_at if bid_drop_at > 0.0 else -1.0
    ask_drop_age = now - ask_drop_at if ask_drop_at > 0.0 else -1.0
    size_ttl = _finite(getattr(screener, "size_signal_ttl_sec", 2.0), 2.0)
    if bid_drop_age < 0.0 or bid_drop_age > size_ttl:
        bid_drop = 0.0
        bid_drop_age = -1.0
    if ask_drop_age < 0.0 or ask_drop_age > size_ttl:
        ask_drop = 0.0
        ask_drop_age = -1.0

    last_order_book_seen = _finite(quote.get("last_order_book_seen"))
    order_book_age = now - last_order_book_seen if last_order_book_seen > 0.0 else -1.0
    bid_level_seen = _finite(quote.get("last_bid_seen"))
    ask_level_seen = _finite(quote.get("last_ask_seen"))
    queue_size_total = bid_size + ask_size
    queue_imbalance_available = bool(bid_size > 0.0 and ask_size > 0.0 and queue_size_total > 0.0)
    queue_imbalance = (bid_size - ask_size) / queue_size_total if queue_imbalance_available else 0.0
    queue_imbalance_age = max(bid_size_age, ask_size_age)

    metrics.update(
        {
            "bid": bid,
            "ask": ask,
            "bid_size": bid_size,
            "ask_size": ask_size,
            "bid_size_available": float(bid_size > 0.0),
            "ask_size_available": float(ask_size > 0.0),
            "order_book_valid": 1.0,
            "queue_imbalance_available": float(queue_imbalance_available),
            "queue_imbalance": queue_imbalance,
            "queue_imbalance_age_sec": round(queue_imbalance_age, 3) if math.isfinite(queue_imbalance_age) else -1.0,
            "bid_size_drop_pct": bid_drop,
            "ask_size_drop_pct": ask_drop,
            "bid_size_baseline": _finite(quote.get("bid_size_baseline")),
            "ask_size_baseline": _finite(quote.get("ask_size_baseline")),
            "bid_size_current": bid_size,
            "ask_size_current": ask_size,
            "top_bid_history_samples": int(_finite(quote.get("top_bid_history_samples"))),
            "top_ask_history_samples": int(_finite(quote.get("top_ask_history_samples"))),
            "bid_size_age_sec": round(bid_size_age, 3) if math.isfinite(bid_size_age) else -1.0,
            "ask_size_age_sec": round(ask_size_age, 3) if math.isfinite(ask_size_age) else -1.0,
            "bid_size_drop_age_sec": round(bid_drop_age, 3) if bid_drop_age >= 0.0 else -1.0,
            "ask_size_drop_age_sec": round(ask_drop_age, 3) if ask_drop_age >= 0.0 else -1.0,
            "bid_ask_available": 1.0,
            "spread_available": 1.0,
            "spread_pct": spread_pct,
            "bid_quote_age_sec": round(bid_age, 3),
            "ask_quote_age_sec": round(ask_age, 3),
            "bid_ask_age_sec": round(bbo_age, 3),
            "bbo_age_sec": round(bbo_age, 3),
            "bbo_max_age_sec": max_age,
            "bid_level_change_age_sec": round(now - bid_level_seen, 3) if bid_level_seen > 0.0 else -1.0,
            "ask_level_change_age_sec": round(now - ask_level_seen, 3) if ask_level_seen > 0.0 else -1.0,
            "order_book_age_sec": round(order_book_age, 3) if order_book_age >= 0.0 else -1.0,
            "order_book_valid": 1.0,
        }
    )
    return True, "Свежий двухсторонний BBO восстановлен по ORDER_BOOK."


def refresh_candidate_bbo_metrics(
    screener: Any,
    ticker: str,
    metrics: dict[str, Any],
    *,
    now: float | None = None,
) -> tuple[bool, str]:
    """Refresh only BBO/visible-best-size metrics from a fresh coherent book.

    The screener's lock is taken when present so the quote and its confirmation
    clocks are read as one consistent snapshot.
    """
    if not isinstance(metrics, dict):
        return False, "Метрики кандидата имеют неверный формат."
    ticker = str(ticker or "").strip().upper().split("@")[0]
    if not ticker:
        return False, "Не указан тикер кандидата."
    timestamp = time.monotonic() if now is None else _finite(now, time.monotonic())
    lock = getattr(screener, "_lock", None)
    if lock is None:
        return _refresh_locked(screener, ticker, metrics, timestamp)
    with lock:
        return _refresh_locked(screener, ticker, metrics, timestamp)


_POST_LLM_TRADE_METRIC_FIELDS = (
    "trade_data_ready",
    "trade_vwap_60s",
    "trade_volume_5s",
    "trade_volume_baseline_5s",
    "volume_spike",
    "volume_spike_raw",
    "trade_baseline_bucket_count",
    "trade_baseline_min_bucket_count",
    "trade_baseline_trade_count",
    "trade_count_5s",
    "price_change_pct",
    "trade_price_change_30s_pct",
    "trade_realized_volatility_60s_pct",
    "trade_flow_imbalance_30s",
    "trade_buy_volume_30s",
    "trade_sell_volume_30s",
    "trade_count_60s",
    "trade_history_coverage_sec",
    "trade_metrics_age_sec",
    "current_price",
    "vwap",
    "price_vs_vwap_pct",
)


def refresh_post_llm_evidence(
    screener: Any,
    ticker: str,
    metrics: Mapping[str, Any],
    *,
    now: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Rebuild the executable BBO and trade-flow evidence immediately after LLM.

    The original pre-LLM signal is retained for attribution, but it is never
    allowed to stand in for the current book or current trade-flow window at the
    final entry gate. Failed refreshes explicitly invalidate the corresponding
    evidence so stale values cannot accidentally pass validation.
    """
    evidence = dict(metrics) if isinstance(metrics, Mapping) else {}
    symbol = str(ticker or "").strip().upper().split("@")[0]
    refreshed_at = time.monotonic() if now is None else _finite(now, time.monotonic())

    try:
        bbo_ready, bbo_reason = refresh_candidate_bbo_metrics(
            screener, symbol, evidence, now=refreshed_at
        )
    except Exception as exc:
        bbo_ready = False
        bbo_reason = f"ORDER_BOOK refresh failed ({type(exc).__name__})."
    if not bbo_ready:
        evidence.update({
            "bid_ask_available": 0.0,
            "spread_available": 0.0,
            "order_book_valid": 0.0,
            "bbo_age_sec": -1.0,
            "bid_ask_age_sec": -1.0,
        })

    diagnostics: dict[str, Any] = {
        "bbo_refresh_status": "PASS" if bbo_ready else "BLOCKED",
        "bbo_refresh_reason": str(bbo_reason)[:240],
        "trade_refresh_status": "NOT_REQUIRED",
        "trade_refresh_reason": "Trade-flow refresh is not enabled for this candidate.",
        "refreshed_at_monotonic": round(refreshed_at, 6),
        "price_refreshed": False,
    }

    trade_mode = _finite(evidence.get("trade_data_mode"))
    if trade_mode >= 0.5:
        try:
            refresh_method = getattr(screener, "refresh_trade_metrics", None)
            if not callable(refresh_method):
                raise RuntimeError("refresh_trade_metrics is unavailable")
            try:
                fresh_trade = refresh_method(symbol, now=refreshed_at)
            except TypeError:
                # Compatibility with test doubles and older screener adapters.
                fresh_trade = refresh_method(symbol)
            if not isinstance(fresh_trade, Mapping):
                raise TypeError("refresh_trade_metrics returned a non-mapping value")
            for field in _POST_LLM_TRADE_METRIC_FIELDS:
                if field in fresh_trade:
                    evidence[field] = fresh_trade[field]
            trade_ready = _finite(evidence.get("trade_data_ready")) >= 0.5
            trade_age = _finite(evidence.get("trade_metrics_age_sec"), -1.0)
            trade_fresh = trade_ready and 0.0 <= trade_age <= 5.0
            diagnostics["trade_refresh_status"] = "PASS" if trade_fresh else (
                "STALE" if trade_ready else "NOT_READY"
            )
            diagnostics["trade_refresh_reason"] = (
                "Fresh INSTRUMENT_TRADES window restored."
                if trade_fresh else
                f"INSTRUMENT_TRADES is not ready/fresh (ready={int(trade_ready)}, age={trade_age:.3f}s)."
            )
            diagnostics["price_refreshed"] = bool(
                trade_fresh and _finite(evidence.get("current_price")) > 0.0
            )
        except Exception as exc:
            evidence.update({
                "trade_data_ready": 0.0,
                "trade_metrics_age_sec": -1.0,
                "volume_spike": 0.0,
                "trade_volume_5s": 0.0,
                "trade_volume_baseline_5s": 0.0,
            })
            diagnostics["trade_refresh_status"] = "ERROR"
            diagnostics["trade_refresh_reason"] = f"INSTRUMENT_TRADES refresh failed ({type(exc).__name__})."

    evidence["post_llm_bbo_refresh_valid"] = float(bbo_ready)
    evidence["post_llm_trade_refresh_valid"] = float(
        diagnostics["trade_refresh_status"] == "PASS"
        if diagnostics["trade_refresh_status"] != "NOT_REQUIRED" else True
    )
    evidence["post_llm_refresh_monotonic"] = refreshed_at
    return evidence, diagnostics


def coalesce_pending_bbo_entry(
    previous: Mapping[str, Any] | None,
    payload: Mapping[str, Any],
    *,
    now: float,
    ttl_sec: float,
) -> tuple[dict[str, Any], str]:
    """Coalesce same-side repeats without extending the original BBO-wait TTL.

    Returns (entry, status), where status is created, coalesced, superseded,
    or expired. A direction change is a genuinely new candidate and may replace
    the prior one; repeated ticks for the same side only refresh its payload.
    """
    current = float(now)
    ttl = max(0.0, float(ttl_sec))
    new_payload = dict(payload)
    if isinstance(new_payload.get("metrics"), Mapping):
        new_payload["metrics"] = dict(new_payload["metrics"])

    if isinstance(previous, Mapping):
        created_at = _finite(previous.get("created_at"), 0.0)
        age = current - created_at
        if created_at > 0.0 and 0.0 <= age <= ttl:
            old_payload = previous.get("payload")
            old_side = (
                str(old_payload.get("candidate_direction") or "").strip().upper()
                if isinstance(old_payload, Mapping) else ""
            )
            new_side = str(new_payload.get("candidate_direction") or "").strip().upper()
            if old_side and old_side == new_side:
                return {
                    "created_at": created_at,
                    "payload": new_payload,
                }, "coalesced"
            return {"created_at": current, "payload": new_payload}, "superseded"
        return {"created_at": current, "payload": new_payload}, "expired"

    return {"created_at": current, "payload": new_payload}, "created"

