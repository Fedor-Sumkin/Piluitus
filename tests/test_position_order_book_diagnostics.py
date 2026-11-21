from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

from monitoring.position_order_book_diagnostics import PositionOrderBookDiagnostics


def _engine(status: str = "OPEN") -> SimpleNamespace:
    return SimpleNamespace(
        paper_positions={
            "SBER": {"ticker": "SBER", "side": "SELL", "status": status}
        }
    )


def _parse_float(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if result == result and abs(result) != float("inf") else None


def test_records_all_order_book_outcomes_and_logs_original_summary(caplog) -> None:
    logger = logging.getLogger("test.position_order_book_diagnostics")
    diagnostics = PositionOrderBookDiagnostics(logger, monotonic=lambda: 100.0)
    engine = _engine()

    diagnostics.record("SBER", {"updated": True, "valid": True}, engine)
    diagnostics.record(
        "SBER",
        {"updated": True, "valid": False, "reason": "crossed_bbo"},
        engine,
    )
    diagnostics.record("SBER", {"updated": False, "out_of_order": True}, engine)
    diagnostics.record("SBER", {"updated": False, "out_of_order": False}, engine)

    screener = SimpleNamespace(
        quotes={
            "SBER": {
                "bid": 100.0,
                "ask": 100.1,
                "last_bid_confirmed_seen": 99.5,
                "last_ask_confirmed_seen": 99.75,
                "last_order_book_event_seen": 99.8,
                "last_order_book_received": 99.9,
                "order_book_valid": True,
                "order_book_last_reason": "OK",
            }
        }
    )

    with caplog.at_level(logging.INFO, logger=logger.name):
        diagnostics.log_open_positions(engine, screener, _parse_float)

    assert "[OPEN POSITION BBO 60s] SBER" in caplog.text
    assert "side=SELL exec=ASK exec_age=0.25s" in caplog.text
    assert "events=4 valid=1 invalid=1 out_of_order=1 unusable=1" in caplog.text
    assert "invalid_reasons=CROSSED_BBO:1" in caplog.text


def test_ignores_non_open_positions_and_clear_resets_counters(caplog) -> None:
    logger = logging.getLogger("test.position_order_book_diagnostics.clear")
    diagnostics = PositionOrderBookDiagnostics(logger, monotonic=lambda: 50.0)
    engine = _engine(status="CLOSED")

    diagnostics.record("SBER", {"updated": True, "valid": False}, engine)
    engine.paper_positions["SBER"]["status"] = "OPEN"
    diagnostics.record("SBER", {"updated": True, "valid": True}, engine)
    diagnostics.clear()

    with caplog.at_level(logging.INFO, logger=logger.name):
        diagnostics.log_open_positions(
            engine,
            SimpleNamespace(quotes={"SBER": {}}),
            _parse_float,
        )

    assert "events=0 valid=0 invalid=0 out_of_order=0 unusable=0" in caplog.text
    assert "invalid_reasons=-" in caplog.text


def test_record_is_thread_safe_for_concurrent_book_updates(caplog) -> None:
    logger = logging.getLogger("test.position_order_book_diagnostics.concurrent")
    diagnostics = PositionOrderBookDiagnostics(logger, monotonic=lambda: 10.0)
    engine = _engine()

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(
            pool.map(
                lambda _: diagnostics.record(
                    "SBER", {"updated": True, "valid": True}, engine
                ),
                range(100),
            )
        )

    with caplog.at_level(logging.INFO, logger=logger.name):
        diagnostics.log_open_positions(
            engine,
            SimpleNamespace(quotes={"SBER": {}}),
            _parse_float,
        )

    assert "events=100 valid=100 invalid=0 out_of_order=0 unusable=0" in caplog.text
