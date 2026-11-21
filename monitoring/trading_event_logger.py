"""Run-scoped asynchronous trading-event journal writer.

This module preserves the historical human-readable journal format while
keeping file I/O off the market-processing event loop.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import threading
from datetime import datetime

from monitoring.logging_setup import RUN_ID, logger

_trading_log_file_lock = threading.Lock()


def _sync_write_trading_log(filename: str, log_line: str) -> None:
    """Append one complete journal block under a process-local file lock."""
    try:
        with _trading_log_file_lock:
            with open(filename, "a", encoding="utf-8") as file:
                file.write(log_line + "\n")
    except Exception as exc:
        logger.error("❌ Ошибка записи в текстовый лог: %s", exc, exc_info=True)


async def log_trading_event_to_file(
    ticker: str,
    price: float,
    decision: str,
    confidence: float,
    tactic: str,
    critic: str,
    *,
    candidate_id: str | None = None,
    verdict_id: str | None = None,
    model_confidence: float | None = None,
    final_confidence: float | None = None,
) -> None:
    """Write one correlated trading decision to the current run's journal."""
    os.makedirs("trading_logs", exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d")
    run_id = re.sub(r"[^A-Za-z0-9_-]", "_", str(RUN_ID or "unknown"))
    filename = f"trading_logs/piluitus_session_{date_str}_{run_id}.txt"

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    model_confidence_value = confidence if model_confidence is None else model_confidence
    final_confidence_text = (
        f"{final_confidence:.1f}%" if final_confidence is not None else "N/A"
    )
    log_line = (
        f"========================================================================\n"
        f"⏰ ВРЕМЯ: {timestamp} | АКТИВ: {ticker} | ЦЕНА: {price} ₽\n"
        f"🧩 ЗАПУСК: {run_id} | candidate_id={candidate_id or 'NONE'} | verdict_id={verdict_id or 'NONE'}\n"
        f"🎯 РЕШЕНИЕ ИИ: {decision} (Модель: {model_confidence_value:.1f}% | "
        f"Финальный фильтр: {final_confidence_text})\n"
        f"------------------------------------------------------------------------\n"
        f"📝 ТАКТИКА: {tactic}\n"
        f"🧐 КРИТИКА: {critic}\n"
        f"========================================================================\n"
    )

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _sync_write_trading_log, filename, log_line)
