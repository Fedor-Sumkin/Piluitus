# -*- coding: utf-8 -*-
import math

MAX_DAILY_LOSS_LIMIT = 0.10

def validate_daily_loss_limit(value: object) -> float:
    """Validate daily-loss fraction; 0.015 means 1.5%; max is 10%."""
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError("Лимит дневного убытка должен быть числом от 0 до 0.10.")
    try:
        limit = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("Некорректное значение лимита дневного убытка.") from exc
    if not math.isfinite(limit):
        raise ValueError("Лимит дневного убытка должен быть конечным числом.")
    if limit <= 0:
        raise ValueError("Лимит дневного убытка должен быть больше нуля.")
    if limit > MAX_DAILY_LOSS_LIMIT:
        raise ValueError("Лимит дневного убытка не может превышать 10% (0.10).")
    return limit
