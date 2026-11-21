"""Behavioral tests for the run-scoped trading event journal."""
from __future__ import annotations

import asyncio
from pathlib import Path

import monitoring.trading_event_logger as trading_event_logger


def _journal_files(directory: Path) -> list[Path]:
    return sorted(directory.glob("piluitus_session_*.txt"))


def test_logger_writes_run_candidate_verdict_and_confidence_fields(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(trading_event_logger, "RUN_ID", "run/with:unsafe*chars")

    asyncio.run(
        trading_event_logger.log_trading_event_to_file(
            "SBER",
            301.25,
            "BUY",
            82.0,
            "momentum continuation",
            "spread and BBO confirmed",
            candidate_id="candidate-17",
            verdict_id="verdict-29",
            model_confidence=82.0,
            final_confidence=76.5,
        )
    )

    files = _journal_files(tmp_path / "trading_logs")
    assert len(files) == 1
    assert files[0].name.endswith("_run_with_unsafe_chars.txt")
    content = files[0].read_text(encoding="utf-8")
    assert "АКТИВ: SBER" in content
    assert "ЦЕНА: 301.25 ₽" in content
    assert "ЗАПУСК: run_with_unsafe_chars" in content
    assert "candidate_id=candidate-17" in content
    assert "verdict_id=verdict-29" in content
    assert "Модель: 82.0%" in content
    assert "Финальный фильтр: 76.5%" in content
    assert "momentum continuation" in content
    assert "spread and BBO confirmed" in content


def test_logger_preserves_optional_confidence_fallbacks(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(trading_event_logger, "RUN_ID", "test-run")

    asyncio.run(
        trading_event_logger.log_trading_event_to_file(
            "GAZP", 165.0, "HOLD", 55.0, "wait", "no confirmation"
        )
    )

    content = _journal_files(tmp_path / "trading_logs")[0].read_text(encoding="utf-8")
    assert "candidate_id=NONE" in content
    assert "verdict_id=NONE" in content
    assert "Модель: 55.0%" in content
    assert "Финальный фильтр: N/A" in content


def test_concurrent_logger_calls_keep_each_journal_block_intact(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(trading_event_logger, "RUN_ID", "parallel-run")

    async def _write_many():
        await asyncio.gather(*(
            trading_event_logger.log_trading_event_to_file(
                f"TICKER-{index}",
                float(index),
                "BUY",
                70.0,
                f"tactic-{index}",
                f"critic-{index}",
                candidate_id=f"candidate-{index}",
                verdict_id=f"verdict-{index}",
            )
            for index in range(12)
        ))

    asyncio.run(_write_many())

    content = _journal_files(tmp_path / "trading_logs")[0].read_text(encoding="utf-8")
    assert content.count("========================================================================") == 24
    for index in range(12):
        assert f"АКТИВ: TICKER-{index}" in content
        assert f"candidate_id=candidate-{index}" in content
        assert f"verdict_id=verdict-{index}" in content
        assert f"critic-{index}" in content
