"""Canonical normalization contracts for incoming market-data events.

Provider payloads are normalized here so the orchestration layer can focus on
routing, diagnostics, screening, and execution decisions.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def normalize_market_ticker(value: Any) -> str:
    """Return a canonical MOEX ticker without its board suffix."""
    return str(value or "").strip().upper().split("@")[0]


def market_event_type(event: Any) -> str:
    """Return the normalized event type for mapping-based provider packets."""
    if not isinstance(event, dict):
        return ""
    return str(event.get("event_type", "")).upper()


@dataclass(frozen=True, slots=True)
class NormalizedQuoteEvent:
    ticker: str
    price: float
    volume: float
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    bid_present: bool
    ask_present: bool
    bid_size_present: bool
    ask_size_present: bool
    day_volume: float
    metrics: Any
    timestamp: Any
    received_monotonic: Any


def normalize_market_quote_event(event: Any) -> NormalizedQuoteEvent:
    """Normalize a QUOTES packet from either dict or attribute-based providers.

    The dict/attribute fallback order intentionally matches the legacy handler.
    """
    if isinstance(event, dict):
        ticker = event.get("security_code") or event.get("ticker", "SBER")
        price = float(event.get("price", 0.0) or 0.0)
        volume = float(event.get("volume", 0.0) or 0.0)
        bid = float(event.get("bid", 0.0) or 0.0)
        ask = float(event.get("ask", 0.0) or 0.0)
        bid_size = float(event.get("bid_size", event.get("bidSize", 0.0)) or 0.0)
        ask_size = float(event.get("ask_size", event.get("askSize", 0.0)) or 0.0)
        bid_present = bool(event.get("bid_present", bid > 0.0))
        ask_present = bool(event.get("ask_present", ask > 0.0))
        bid_size_present = bool(event.get("bid_size_present", bid_size > 0.0))
        ask_size_present = bool(event.get("ask_size_present", ask_size > 0.0))
        day_volume = float(event.get("day_volume", 0.0) or 0.0)
        metrics = event.get("metrics", {})
        timestamp = event.get("timestamp") or event.get("datetime") or event.get("time")
        received_monotonic = event.get("received_monotonic")
    else:
        ticker = getattr(event, "security_code", getattr(event, "ticker", "SBER"))
        price = float(getattr(event, "price", 0.0) or 0.0)
        volume = float(getattr(event, "volume", 0.0) or 0.0)
        bid = float(getattr(event, "bid", 0.0) or 0.0)
        ask = float(getattr(event, "ask", 0.0) or 0.0)
        bid_size = float(getattr(event, "bid_size", getattr(event, "bidSize", 0.0)) or 0.0)
        ask_size = float(getattr(event, "ask_size", getattr(event, "askSize", 0.0)) or 0.0)
        bid_present = bool(getattr(event, "bid_present", bid > 0.0))
        ask_present = bool(getattr(event, "ask_present", ask > 0.0))
        bid_size_present = bool(getattr(event, "bid_size_present", bid_size > 0.0))
        ask_size_present = bool(getattr(event, "ask_size_present", ask_size > 0.0))
        day_volume = float(getattr(event, "day_volume", 0.0) or 0.0)
        metrics = getattr(event, "metrics", {})
        timestamp = (
            getattr(event, "timestamp", None)
            or getattr(event, "datetime", None)
            or getattr(event, "time", None)
        )
        received_monotonic = getattr(event, "received_monotonic", None)

    return NormalizedQuoteEvent(
        ticker=normalize_market_ticker(ticker),
        price=price,
        volume=volume,
        bid=bid,
        ask=ask,
        bid_size=bid_size,
        ask_size=ask_size,
        bid_present=bid_present,
        ask_present=ask_present,
        bid_size_present=bid_size_present,
        ask_size_present=ask_size_present,
        day_volume=day_volume,
        metrics=metrics,
        timestamp=timestamp,
        received_monotonic=received_monotonic,
    )


def normalize_order_book_event(event: dict[str, Any]) -> dict[str, Any]:
    """Build the exact keyword payload accepted by HFTMemoryScreener.push_order_book."""
    if not isinstance(event, dict):
        raise TypeError("ORDER_BOOK event must be a dict")

    return {
        "ticker": normalize_market_ticker(
            event.get("ticker") or event.get("security_code") or ""
        ),
        "bid": float(event.get("bid", 0.0) or 0.0),
        "ask": float(event.get("ask", 0.0) or 0.0),
        "bid_size": float(event.get("bid_size", 0.0) or 0.0),
        "ask_size": float(event.get("ask_size", 0.0) or 0.0),
        "bid_present": bool(event.get("bid_present", True)),
        "ask_present": bool(event.get("ask_present", True)),
        "bid_size_present": bool(event.get("bid_size_present", True)),
        "ask_size_present": bool(event.get("ask_size_present", True)),
        "bid_timestamp": event.get("bid_timestamp") or event.get("order_book_timestamp"),
        "ask_timestamp": event.get("ask_timestamp") or event.get("order_book_timestamp"),
        "order_book_timestamp": event.get("order_book_timestamp"),
        "received_monotonic": event.get("received_monotonic"),
        "book_valid": bool(event.get("book_valid", True)),
        "book_reason": str(event.get("book_reason") or ""),
        "bid_levels": event.get("bid_levels") if isinstance(event.get("bid_levels"), list) else None,
        "ask_levels": event.get("ask_levels") if isinstance(event.get("ask_levels"), list) else None,
        "bid_levels_present": bool(event.get("bid_levels_present", False)),
        "ask_levels_present": bool(event.get("ask_levels_present", False)),
    }


def build_market_candidate_payload(
    *,
    ticker: str,
    candidate_created_monotonic: Any,
    price: float,
    volume: float,
    bid: float,
    ask: float,
    bid_size: float,
    ask_size: float,
    day_volume: float,
    candidate_direction: str,
    shadow_candidate_id: Any,
    paper_session_generation: Any,
    metrics: Any,
) -> dict[str, Any]:
    """Create a detached candidate payload with orchestration-only metrics removed."""
    source_metrics = metrics if isinstance(metrics, dict) else {}
    return {
        "ticker": ticker,
        "candidate_created_monotonic": candidate_created_monotonic,
        "price": price,
        "volume": volume,
        "bid": bid,
        "ask": ask,
        "bid_size": bid_size,
        "ask_size": ask_size,
        "day_volume": day_volume,
        "candidate_direction": candidate_direction,
        "shadow_candidate_id": shadow_candidate_id,
        "paper_session_generation": paper_session_generation,
        "metrics": {
            key: value
            for key, value in source_metrics.items()
            if key not in {"selected_for_ai", "funnel_ready"}
        },
    }
