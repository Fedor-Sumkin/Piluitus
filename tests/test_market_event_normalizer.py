from __future__ import annotations

from types import SimpleNamespace

import pytest

from runtime.market_event_normalizer import (
    build_market_candidate_payload,
    market_event_type,
    normalize_market_quote_event,
    normalize_market_ticker,
    normalize_order_book_event,
)


def test_normalize_market_ticker_removes_board_suffix_and_whitespace():
    assert normalize_market_ticker(" sber@tqbr ") == "SBER"
    assert normalize_market_ticker(None) == ""
    assert normalize_market_ticker("gazp") == "GAZP"


def test_market_event_type_routes_mapping_packets_case_insensitively():
    assert market_event_type({"event_type": "order_book"}) == "ORDER_BOOK"
    assert market_event_type({"event_type": None}) == "NONE"
    assert market_event_type(SimpleNamespace(event_type="ORDER_BOOK")) == ""


def test_normalize_quote_event_supports_legacy_size_aliases_and_timestamp_fallback():
    metrics = {"impulse": 1.2}
    event = {
        "ticker": " sber@TQBR ",
        "price": "101.25",
        "volume": "12",
        "bid": "101.2",
        "ask": "101.3",
        "bidSize": "7",
        "askSize": "9",
        "day_volume": "1500",
        "datetime": "2026-11-21T10:00:00",
        "received_monotonic": 12.5,
        "metrics": metrics,
    }

    normalized = normalize_market_quote_event(event)

    assert normalized.ticker == "SBER"
    assert (normalized.price, normalized.volume) == (101.25, 12.0)
    assert (normalized.bid, normalized.ask) == (101.2, 101.3)
    assert (normalized.bid_size, normalized.ask_size) == (7.0, 9.0)
    assert (normalized.bid_present, normalized.ask_present) == (True, True)
    assert (normalized.bid_size_present, normalized.ask_size_present) == (True, True)
    assert normalized.day_volume == 1500.0
    assert normalized.timestamp == "2026-11-21T10:00:00"
    assert normalized.received_monotonic == 12.5
    assert normalized.metrics is metrics


def test_normalize_quote_event_preserves_explicit_false_presence_flags():
    normalized = normalize_market_quote_event({
        "security_code": "GAZP",
        "price": 10,
        "volume": 0,
        "bid": 9.9,
        "ask": 10.1,
        "bid_size": 5,
        "ask_size": 6,
        "bid_present": False,
        "ask_present": True,
        "bid_size_present": False,
        "ask_size_present": True,
        "timestamp": "quote-ts",
        "time": "fallback-ts",
    })

    assert normalized.ticker == "GAZP"
    assert normalized.bid_present is False
    assert normalized.ask_present is True
    assert normalized.bid_size_present is False
    assert normalized.ask_size_present is True
    assert normalized.timestamp == "quote-ts"


def test_normalize_quote_event_supports_attribute_based_provider_objects():
    event = SimpleNamespace(
        ticker="LKOH@TQBR",
        price=7000,
        volume=4,
        bid=6999,
        ask=7001,
        bid_size=3,
        ask_size=5,
        day_volume=100,
        timestamp="event-ts",
        received_monotonic=4.5,
        metrics={"source": "object"},
    )

    normalized = normalize_market_quote_event(event)

    assert normalized.ticker == "LKOH"
    assert normalized.price == 7000.0
    assert normalized.volume == 4.0
    assert normalized.timestamp == "event-ts"
    assert normalized.received_monotonic == 4.5
    assert normalized.metrics == {"source": "object"}


def test_normalize_quote_event_uses_default_ticker_for_missing_provider_field():
    normalized = normalize_market_quote_event({"price": 1, "volume": 0})
    assert normalized.ticker == "SBER"


def test_normalize_quote_event_does_not_hide_numeric_conversion_errors():
    with pytest.raises((TypeError, ValueError)):
        normalize_market_quote_event({"ticker": "SBER", "price": "invalid"})


def test_normalize_order_book_event_builds_screener_keyword_payload():
    event = {
        "security_code": " hydr@tqbr ",
        "bid": "10.1",
        "ask": "10.2",
        "bid_size": "100",
        "ask_size": "120",
        "order_book_timestamp": "book-ts",
        "received_monotonic": 55.0,
        "book_reason": None,
        "bid_levels": [{"price": 10.1, "volume": 100}],
        "ask_levels": "not-a-level-list",
        "bid_levels_present": True,
    }

    normalized = normalize_order_book_event(event)

    assert normalized["ticker"] == "HYDR"
    assert normalized["bid"] == 10.1
    assert normalized["ask"] == 10.2
    assert normalized["bid_size"] == 100.0
    assert normalized["ask_size"] == 120.0
    assert normalized["bid_timestamp"] == "book-ts"
    assert normalized["ask_timestamp"] == "book-ts"
    assert normalized["order_book_timestamp"] == "book-ts"
    assert normalized["received_monotonic"] == 55.0
    assert normalized["book_valid"] is True
    assert normalized["book_reason"] == ""
    assert normalized["bid_levels"] == [{"price": 10.1, "volume": 100}]
    assert normalized["ask_levels"] is None
    assert normalized["bid_levels_present"] is True
    assert normalized["ask_levels_present"] is False


def test_normalize_order_book_event_preserves_explicit_invalidity_and_absent_sides():
    normalized = normalize_order_book_event({
        "ticker": "SBER",
        "bid_present": False,
        "ask_present": False,
        "bid_size_present": False,
        "ask_size_present": False,
        "book_valid": False,
        "book_reason": "empty snapshot",
    })

    assert normalized["bid_present"] is False
    assert normalized["ask_present"] is False
    assert normalized["bid_size_present"] is False
    assert normalized["ask_size_present"] is False
    assert normalized["book_valid"] is False
    assert normalized["book_reason"] == "empty snapshot"


def test_normalize_order_book_event_rejects_non_mapping_input():
    with pytest.raises(TypeError):
        normalize_order_book_event(SimpleNamespace(ticker="SBER"))


def test_normalize_order_book_event_does_not_hide_numeric_conversion_errors():
    with pytest.raises((TypeError, ValueError)):
        normalize_order_book_event({"ticker": "SBER", "bid": "invalid"})


def test_market_candidate_payload_filters_runtime_flags_and_copies_metrics():
    metrics = {
        "price_change_pct": 0.3,
        "selected_for_ai": True,
        "funnel_ready": True,
        "queue_imbalance_5s": 0.4,
    }

    payload = build_market_candidate_payload(
        ticker="SBER",
        candidate_created_monotonic=12.5,
        price=101.0,
        volume=10.0,
        bid=100.9,
        ask=101.1,
        bid_size=20.0,
        ask_size=25.0,
        day_volume=1000.0,
        candidate_direction="BUY",
        shadow_candidate_id="shadow-1",
        paper_session_generation=7,
        metrics=metrics,
    )

    assert payload == {
        "ticker": "SBER",
        "candidate_created_monotonic": 12.5,
        "price": 101.0,
        "volume": 10.0,
        "bid": 100.9,
        "ask": 101.1,
        "bid_size": 20.0,
        "ask_size": 25.0,
        "day_volume": 1000.0,
        "candidate_direction": "BUY",
        "shadow_candidate_id": "shadow-1",
        "paper_session_generation": 7,
        "metrics": {
            "price_change_pct": 0.3,
            "queue_imbalance_5s": 0.4,
        },
    }
    assert "selected_for_ai" in metrics
    assert "funnel_ready" in metrics
    assert payload["metrics"] is not metrics


def test_market_candidate_payload_tolerates_non_dict_metrics():
    payload = build_market_candidate_payload(
        ticker="GAZP",
        candidate_created_monotonic=1.0,
        price=10.0,
        volume=1.0,
        bid=9.9,
        ask=10.1,
        bid_size=5.0,
        ask_size=6.0,
        day_volume=100.0,
        candidate_direction="SELL",
        shadow_candidate_id=None,
        paper_session_generation=2,
        metrics=None,
    )
    assert payload["metrics"] == {}
