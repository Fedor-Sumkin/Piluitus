"""Read-only trading status snapshot for the operator dashboard."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

_MOSCOW_TZ = timezone(timedelta(hours=3), name="MSK")

_REASON_MESSAGES = {
    "LOSS_STREAK_HARD_STOP": (
        "Достигнут лимит последовательных убыточных сделок. "
        "Новые входы заблокированы до следующей торговой даты по Москве; "
        "уже открытые позиции продолжают сопровождаться."
    ),
    "DAILY_LOSS_LIMIT": (
        "Достигнут дневной лимит убытка. Новые входы заблокированы до следующей "
        "торговой даты по Москве; открытые позиции продолжают сопровождаться."
    ),
    "EQUITY_DRAWDOWN": (
        "Достигнут лимит просадки капитала от максимума. Новые входы заблокированы; "
        "открытые позиции продолжают сопровождаться."
    ),
    "RISK_CHECK_ERROR": (
        "Проверка риск-ограничений завершилась ошибкой. Включена безопасная блокировка "
        "новых входов; открытые позиции продолжают сопровождаться."
    ),
    "RISK_GUARD": (
        "Риск-менеджер заблокировал новые входы. Подробности доступны в журнале событий; "
        "открытые позиции продолжают сопровождаться."
    ),
}


def _safe_int(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def build_trading_status_snapshot(
    *, risk_manager: Any, execution_engine: Any
) -> dict[str, Any]:
    """Describe the real trading gate and stream state; never changes either one."""
    paper_mode = bool(getattr(execution_engine, "paper_mode", True))
    mode = "PAPER" if paper_mode else "LIVE"
    broker = getattr(execution_engine, "broker", None)
    stream_running = bool(getattr(broker, "is_ws_running", False)) if broker is not None else False
    market_connected = bool(
        getattr(broker, "_market_stream_connected", False)
    ) if broker is not None else False

    loss_streak = max(0, _safe_int(getattr(risk_manager, "_loss_streak_count", 0), 0))
    loss_limit = max(1, _safe_int(getattr(risk_manager, "loss_streak_hard_stop", 5), 5))
    risk_allowed = getattr(risk_manager, "_cached_trading_allowed", None)
    block_trading = bool(getattr(risk_manager, "block_trading", False))
    reason = str(getattr(risk_manager, "_trading_block_reason", "") or "").strip().upper()

    pause_until = getattr(risk_manager, "_loss_streak_pause_until", None)
    pause_remaining = 0
    pause_until_text = None
    if isinstance(pause_until, datetime):
        pause_until_msk = (
            pause_until.astimezone(_MOSCOW_TZ).replace(tzinfo=None)
            if pause_until.tzinfo is not None else pause_until
        )
        pause_remaining = max(
            0, int((pause_until_msk - datetime.now(_MOSCOW_TZ).replace(tzinfo=None)).total_seconds())
        )
        pause_until_text = pause_until.isoformat(timespec="seconds")

    if block_trading or reason in {
        "LOSS_STREAK_HARD_STOP", "DAILY_LOSS_LIMIT", "EQUITY_DRAWDOWN",
        "RISK_CHECK_ERROR", "RISK_GUARD",
    }:
        status = "STOPPED"
        title = "Новые входы остановлены"
        message = _REASON_MESSAGES.get(
            reason,
            "Новые входы заблокированы риск-менеджером; открытые позиции продолжают сопровождаться.",
        )
    elif reason == "LOSS_STREAK_PAUSE":
        if pause_remaining > 0:
            status = "PAUSED"
            title = "Торговля временно приостановлена"
            message = (
                f"Серия из {loss_streak} убыточных сделок подряд: новые входы на паузе ещё "
                f"примерно {pause_remaining} сек. Открытые позиции продолжают сопровождаться."
            )
        else:
            status = "CHECKING"
            title = "Пауза завершена — проверка допуска"
            message = "Ожидается повторная проверка риск-менеджера; новые входы пока не подтверждены."
    elif risk_allowed is False:
        status = "STOPPED"
        title = "Новые входы заблокированы"
        message = _REASON_MESSAGES.get(
            reason,
            "Риск-менеджер пока не разрешает новые входы; открытые позиции продолжают сопровождаться.",
        )
    elif not market_connected:
        status = "CONNECTING"
        title = "Подключение к рыночному потоку"
        message = (
            "Ожидается подтверждённое WebSocket-соединение Finam (HANDSHAKE_SUCCESS). "
            "До него статус допуска к новым входам не считается активным."
            if stream_running else
            "Поток котировок ещё не запущен или переподключается. Ожидается авторизация Finam и HANDSHAKE_SUCCESS."
        )
    elif risk_allowed is None:
        status = "CHECKING"
        title = "Проверка торговых ограничений"
        message = "Рыночный поток подключён. Ожидается первая актуальная проверка риск-менеджера."
    else:
        status = "ACTIVE"
        title = "Система активна — новые входы разрешены"
        message = (
            f"Риск-ограничения разрешают новые входы в режиме {mode}. "
            "Скринер анализирует рынок; открытые позиции продолжают сопровождаться."
        )

    return {
        "status": status,
        "title": title,
        "message": message,
        "mode": mode,
        "market_connected": market_connected,
        "market_stream_running": stream_running,
        "trading_allowed": status == "ACTIVE",
        "loss_streak": loss_streak,
        "loss_streak_limit": loss_limit,
        "block_reason": reason or None,
        "pause_until": pause_until_text,
        "pause_remaining_seconds": pause_remaining,
    }
