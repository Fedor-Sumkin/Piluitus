from __future__ import annotations

import logging

from monitoring.prefilter_diagnostics import PrefilterBlockLogger


def test_prefilter_logger_rate_limits_same_ticker_and_reason_bucket(caplog) -> None:
    now = [20.0]
    logger = logging.getLogger("test.prefilter_diagnostics.rate_limit")
    limiter = PrefilterBlockLogger(logger, monotonic=lambda: now[0])

    with caplog.at_level(logging.WARNING, logger=logger.name):
        assert limiter.log("sber", "Спред выше допустимого")
        now[0] = 30.0
        assert not limiter.log("SBER", "Спред слишком широкий")
        assert limiter.log("SBER", "Сокращение объёма лучшей заявки ниже порога")

    records = [record for record in caplog.records if record.name == logger.name]
    assert len(records) == 2
    assert "[PRE-LLM FILTER] sber пропущен до запроса LLM (spread)" in records[0].getMessage()
    assert "(size)" in records[1].getMessage()


def test_prefilter_logger_reopens_after_interval_and_clear(caplog) -> None:
    now = [100.0]
    logger = logging.getLogger("test.prefilter_diagnostics.reset")
    limiter = PrefilterBlockLogger(logger, monotonic=lambda: now[0])

    with caplog.at_level(logging.WARNING, logger=logger.name):
        assert limiter.log("GAZP", "Bid/Ask неактуальны")
        now[0] = 114.99
        assert not limiter.log("GAZP", "Котировки устарели")
        now[0] = 115.0
        assert limiter.log("GAZP", "Котировки устарели")
        limiter.clear()
        assert limiter.log("GAZP", "Котировки устарели")

    assert len([record for record in caplog.records if record.name == logger.name]) == 3


def test_prefilter_logger_keeps_tickers_and_categories_independent(caplog) -> None:
    now = [50.0]
    logger = logging.getLogger("test.prefilter_diagnostics.keys")
    limiter = PrefilterBlockLogger(logger, monotonic=lambda: now[0])

    with caplog.at_level(logging.WARNING, logger=logger.name):
        assert limiter.log("SBER", "Спред выше порога")
        assert limiter.log("GAZP", "Спред выше порога")
        assert limiter.log("SBER", "Размер лучшей заявки недостаточен")

    assert len([record for record in caplog.records if record.name == logger.name]) == 3


def test_m1_conflict_guard_has_a_separate_diagnostic_bucket() -> None:
    from monitoring.trade_flow_diagnostics import guard_reason_bucket

    reason = (
        "PAPER-вход заблокирован: подтверждённый закрытыми M1-свечами "
        "тренд/паттерн противоречит направлению BUY."
    )
    assert guard_reason_bucket(reason) == "m1"
