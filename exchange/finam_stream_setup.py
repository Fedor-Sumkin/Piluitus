"""WebSocket API compatibility and QUOTES subscription planning."""
from __future__ import annotations
import inspect
import os
from collections.abc import Iterable
from typing import Any

def resolve_websocket_header_argument(connect_callable: Any) -> str:
    try:
        parameters = inspect.signature(connect_callable).parameters
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            "Не удалось определить API установленного пакета websockets. "
            "Обновите пакет командой: python -m pip install -U websockets"
        ) from exc
    if "additional_headers" in parameters:
        return "additional_headers"
    if "extra_headers" in parameters:
        return "extra_headers"
    raise RuntimeError(
        "Установленная версия websockets не поддерживает дополнительные HTTP-заголовки. "
        "Выполните: python -m pip install -U websockets"
    )

def build_quote_batches(symbols: Iterable[str], batch_size: int) -> list[list[str]]:
    size = int(batch_size)
    if size < 1:
        raise ValueError("Размер пакета QUOTES должен быть не меньше 1.")
    values = list(symbols)
    return [values[index:index + size] for index in range(0, len(values), size)]

def build_quote_subscription_plan(
    tickers: Iterable[object], *, batch_size_value: object | None = None
) -> tuple[list[str], int, list[list[str]]]:
    symbols: list[str] = []
    seen: set[str] = set()
    for raw_ticker in tickers:
        symbol = str(raw_ticker).strip()
        if not symbol:
            continue
        if "@" not in symbol:
            symbol = f"{symbol}@MISX"
        if symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)
    if not symbols:
        raise ValueError("Список TICKERS_TO_TRADE пуст: нечего подписывать на Finam QUOTES.")
    raw_size = os.getenv("FINAM_WS_SYMBOL_BATCH_SIZE", "10") if batch_size_value is None else batch_size_value
    try:
        batch_size = int(raw_size)
    except (TypeError, ValueError) as exc:
        raise ValueError("FINAM_WS_SYMBOL_BATCH_SIZE должен быть целым числом, например 10.") from exc
    if batch_size < 1:
        raise ValueError("FINAM_WS_SYMBOL_BATCH_SIZE должен быть не меньше 1.")
    return symbols, batch_size, build_quote_batches(symbols, batch_size)
