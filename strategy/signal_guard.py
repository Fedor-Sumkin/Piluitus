"""Deterministic evidence gate for LLM-generated scalping decisions.

This module intentionally treats the LLM as a candidate signal proposer, not as
an authority on facts which were not measured by the quote stream.
"""
from __future__ import annotations

import math
import re
import time
import os
from typing import Any

MIN_VOLUME_SPIKE = 1.5
MIN_TOP_SIZE_DROP_PCT = 50.0
MIN_SIZE_HISTORY_SAMPLES = 3
MAX_SPREAD_PCT = 0.50
MAX_SIZE_SIGNAL_AGE_SEC = 2.0
try:
    MAX_BAR_PATTERN_SIGNAL_AGE_SEC = max(2.0, float(os.getenv("PILUITUS_PATTERN_SIGNAL_MAX_AGE_SEC", "15")))
except (TypeError, ValueError):
    MAX_BAR_PATTERN_SIGNAL_AGE_SEC = 15.0
ALLOWED_BAR_PATTERNS = {
    "M1_RANGE_BREAKOUT_BUY", "M1_RANGE_BREAKOUT_SELL",
    "M1_EMA_PULLBACK_BUY", "M1_EMA_PULLBACK_SELL",
}


def _num(metrics: dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        value = float(metrics.get(key, default))
        return value if math.isfinite(value) else default
    except (TypeError, ValueError):
        return default


def _fresh_bar_pattern_direction(metrics: dict[str, Any]) -> str | None:
    """Return a supported 1m-pattern direction only while its trigger is fresh."""
    if _num(metrics, "bar_pattern_confirmed") < 0.5:
        return None
    name = str(metrics.get("bar_pattern_name", "") or "").strip().upper()
    if name not in ALLOWED_BAR_PATTERNS:
        return None
    required_flag = {
        "M1_RANGE_BREAKOUT_BUY": "bar_breakout_up",
        "M1_RANGE_BREAKOUT_SELL": "bar_breakout_down",
        "M1_EMA_PULLBACK_BUY": "bar_pullback_buy",
        "M1_EMA_PULLBACK_SELL": "bar_pullback_sell",
    }[name]
    if _num(metrics, required_flag) < 0.5:
        return None
    signal_at = _num(metrics, "_bar_pattern_signal_at_monotonic", -1.0)
    if signal_at <= 0.0:
        return None
    age = time.monotonic() - signal_at
    if age < -0.25 or age > MAX_BAR_PATTERN_SIGNAL_AGE_SEC:
        return None
    code = _num(metrics, "bar_pattern_direction_code")
    expected_direction = "BUY" if name.endswith("_BUY") else "SELL"
    if expected_direction == "BUY" and code >= 0.5:
        return "BUY"
    if expected_direction == "SELL" and code <= -0.5:
        return "SELL"
    return None


def validate_atr_readiness(metrics: dict[str, Any]) -> tuple[bool, str]:
    """Check that a valid completed-M1 ATR exists before spending an LLM call."""
    try:
        min_periods = min(14, max(2, int(os.getenv("PILUITUS_ATR_MIN_PERIODS", "5"))))
    except (TypeError, ValueError):
        min_periods = 5
    atr = _num(metrics, "bar_atr")
    periods = max(0, int(_num(metrics, "bar_atr_periods")))
    if not math.isfinite(atr) or atr <= 0.0 or periods < min_periods:
        return False, (
            f"Недостаточно закрытых M1-диапазонов для ATR: {periods}/{min_periods}; "
            "кандидат не отправлен в LLM."
        )
    return True, "Достаточная M1-история для расчёта ATR подтверждена."


def infer_candidate_direction(metrics: dict[str, Any]) -> str | None:
    """Use a fresh, closed 1m pattern first; otherwise use momentum + VWAP.

    This stage proposes a direction only. Spread, quote freshness, relative
    last-trade size, risk, capital and execution guards remain independent.
    """
    if not isinstance(metrics, dict):
        return None
    pattern_direction = _fresh_bar_pattern_direction(metrics)
    change = _num(metrics, "price_change_pct", 0.0)
    vs_vwap = _num(metrics, "price_vs_vwap_pct", 0.0)
    if pattern_direction == "BUY":
        return "BUY" if vs_vwap > 0.0 else None
    if pattern_direction == "SELL":
        return "SELL" if vs_vwap < 0.0 else None
    if change > 0.0 and vs_vwap > 0.0:
        return "BUY"
    if change < 0.0 and vs_vwap < 0.0:
        return "SELL"
    return None


def validate_signal_evidence(decision: str, metrics: dict[str, Any]) -> tuple[bool, str]:
    """Allow BUY/SELL only with a fresh measured setup and market-quality evidence.

    A valid closed 1m breakout/EMA pullback can replace the top-quote size-drop
    trigger, but never the fresh BBO, spread, relative last-trade-size, VWAP and
    risk checks. Top-quote sizes are not full depth-of-book.
    """
    decision = str(decision or "HOLD").strip().upper()
    if decision not in {"BUY", "SELL"}:
        return True, ""
    if not isinstance(metrics, dict):
        return False, "Нет структурированных рыночных метрик; вход заблокирован."

    required_common = (
        "price_change_pct", "price_vs_vwap_pct", "volume_spike", "spread_pct",
        "spread_available", "bid_ask_available",
    )
    missing = [key for key in required_common if key not in metrics]
    if missing:
        return False, "Нет обязательных метрик направления/спреда; вход заблокирован."
    if _num(metrics, "spread_available") < 0.5 or _num(metrics, "bid_ask_available") < 0.5:
        bid_age = _num(metrics, "bid_quote_age_sec", -1.0)
        ask_age = _num(metrics, "ask_quote_age_sec", -1.0)
        bbo_age = _num(metrics, "bbo_age_sec", _num(metrics, "bid_ask_age_sec", -1.0))
        max_age = _num(metrics, "bbo_max_age_sec", 2.0)
        if bid_age >= 0.0 or ask_age >= 0.0 or bbo_age >= 0.0:
            bid_label = f"{bid_age:.2f}s" if bid_age >= 0.0 else "нет"
            ask_label = f"{ask_age:.2f}s" if ask_age >= 0.0 else "нет"
            bbo_label = f"{bbo_age:.2f}s" if bbo_age >= 0.0 else "нет"
            bid_level_age = _num(metrics, "bid_level_change_age_sec", -1.0)
            ask_level_age = _num(metrics, "ask_level_change_age_sec", -1.0)
            level_detail = ""
            if bid_level_age >= 0.0 or ask_level_age >= 0.0:
                bid_level_label = f"{bid_level_age:.2f}s" if bid_level_age >= 0.0 else "нет"
                ask_level_label = f"{ask_level_age:.2f}s" if ask_level_age >= 0.0 else "нет"
                level_detail = f"; последнее изменение уровня bid={bid_level_label}, ask={ask_level_label}"
            return False, (
                f"Bid/Ask не подтверждены потоком: возраст подтверждения BBO={bbo_label}; "
                f"возраст подтверждения сторон bid={bid_label}, ask={ask_label}{level_detail}; "
                f"лимит BBO {max_age:.2f}s. Вход заблокирован."
            )
        return False, "Bid/Ask отсутствуют в потоке; вход заблокирован."
    spread_pct = _num(metrics, "spread_pct", -1.0)
    if spread_pct < 0.0 or spread_pct > MAX_SPREAD_PCT:
        return False, f"Спред не проходит фильтр: {spread_pct:.3f}% при лимите {MAX_SPREAD_PCT:.2f}%."
    volume_spike = _num(metrics, "volume_spike")
    if volume_spike < MIN_VOLUME_SPIKE:
        return False, f"Всплеск размера последней сделки {volume_spike:.2f}× ниже порога {MIN_VOLUME_SPIKE:.1f}×."

    price_change = _num(metrics, "price_change_pct")
    price_vs_vwap = _num(metrics, "price_vs_vwap_pct")
    pattern_direction = _fresh_bar_pattern_direction(metrics)
    if pattern_direction is not None:
        if pattern_direction != decision:
            return False, f"Направление 1-минутного паттерна {pattern_direction} не совпадает с решением {decision}."
        if decision == "BUY" and price_vs_vwap <= 0.0:
            return False, "BUY по 1-минутному паттерну заблокирован: цена не выше VWAP."
        if decision == "SELL" and price_vs_vwap >= 0.0:
            return False, "SELL по 1-минутному паттерну заблокирован: цена не ниже VWAP."
        # The measured candle setup is an alternative to a top-quote size drop.
        # Fresh BBO, a narrow spread and the current last-trade-size spike are
        # still mandatory; OHLC alone never overrides those protections.
        return True, (
            f"Подтверждён минутный паттерн {str(metrics.get('bar_pattern_name', '')).strip()}; "
            "VWAP, всплеск размера последней сделки, свежий BBO и спред прошли проверку."
        )

    if decision == "BUY":
        if price_change <= 0.0 or price_vs_vwap <= 0.0:
            return False, "BUY заблокирован: цена не растёт одновременно с положительным отклонением от VWAP."
        drop_key, samples_key, available_key, side_name = (
            "ask_size_drop_pct", "top_ask_history_samples", "ask_size_available", "Ask"
        )
    else:
        if price_change >= 0.0 or price_vs_vwap >= 0.0:
            return False, "SELL заблокирован: нет одновременно отрицательного импульса и цены ниже VWAP."
        drop_key, samples_key, available_key, side_name = (
            "bid_size_drop_pct", "top_bid_history_samples", "bid_size_available", "Bid"
        )

    if _num(metrics, available_key) < 0.5:
        return False, f"Размер лучшей заявки {side_name} недоступен; вход заблокирован."
    samples = int(_num(metrics, samples_key))
    if samples < MIN_SIZE_HISTORY_SAMPLES:
        return False, f"Недостаточно истории размера лучшей заявки {side_name}: {samples}/{MIN_SIZE_HISTORY_SAMPLES} наблюдений."
    drop_pct = _num(metrics, drop_key)
    baseline = _num(metrics, "ask_size_baseline" if decision == "BUY" else "bid_size_baseline")
    current_size = _num(metrics, "ask_size_current" if decision == "BUY" else "bid_size_current")
    drop_age_key = "ask_size_drop_age_sec" if decision == "BUY" else "bid_size_drop_age_sec"
    drop_age = _num(metrics, drop_age_key, -1.0)
    diagnostic = (
        f"текущий размер={current_size:.2f}, медиана предыдущих={baseline:.2f}, "
        f"история={samples}, возраст сигнала={drop_age:.2f}s"
        if baseline > 0.0 else
        f"текущий размер={current_size:.2f}, предыдущая медиана недоступна, "
        f"история={samples}, возраст сигнала={drop_age:.2f}s"
    )
    if drop_pct < MIN_TOP_SIZE_DROP_PCT:
        return False, (
            f"Сокращение объёма лучшей заявки {side_name} {drop_pct:.1f}% меньше порога "
            f"{MIN_TOP_SIZE_DROP_PCT:.0f}% ({diagnostic})."
        )
    if drop_age < 0.0 or drop_age > MAX_SIZE_SIGNAL_AGE_SEC:
        age_label = f"{drop_age:.2f}s" if drop_age >= 0.0 else "не зафиксировано"
        return False, (
            f"Сокращение размера лучшей заявки {side_name} не подтверждено как свежее: "
            f"возраст={age_label}, лимит {MAX_SIZE_SIGNAL_AGE_SEC:.2f}s."
        )

    return True, "Проверяемые условия направления, всплеска объёма, спреда и недавнего сокращения размера выполнены."


_CORRUPT_MARKERS = re.compile(
    r"\bregex\b|vendorsky|на\s*-\s*[a-zа-яё]+|на[a-z]{5,}|"
    r"на\s+[а-яё]{1,2}\s*:|на\s+ном\s+риске|"
    r"[a-z]{4,}['’]s\s+pattern|\b(?:null|undefined)\b|"
    r"\bneither\b|\btrapping\b|\b(?:metriки|decide|shortselling)\b|"
    r"куп(?:ку|ки|он(?:у)?)\s*(?:или|/)\s*продаж",
    re.IGNORECASE,
)
_STRONG_UNMEASURED_CLAIMS = re.compile(
    r"лавинообраз|исчезнов|разъедан|плотност|агрессивн.{0,20}(?:поток|покуп|продав)|"
    r"дельт[ауы]?|полный стакан|глубин.{0,10}стакан|отмена лимитн.{0,12}заявк|"
    r"спред.{0,30}(?:уменьш|сниз|увелич|расшир|суж|сократ|раст[её]т)|"
    r"(?:восходящ|нисходящ|сильн|устойчив).{0,15}тренд|"
    r"давлени.{0,20}(?:покупател|продавц)|(?:покупател|продавц).{0,20}давлени|"
    r"(?:поддержк|сопротивлен).{0,30}(?:обнаруж|сильн|держит|уровень)|"
    r"\b(?:signed\s+delta|aggressive\s+(?:flow|buyers|sellers)|full\s+order[- ]book|"
    r"order[- ]book\s+depth|liquidity\s+disappear(?:s|ed)?|avalanche)\b",
    re.IGNORECASE,
)
_SIDE_SIZE_CLAIM = re.compile(
    r"(?:ask|аск|bid|бид|лучш.{0,12}котировк).{0,55}(?:объ[её]м|размер|заявк|сократ|уменьш|плотност|исчезнов)|"
    r"(?:объ[её]м|размер|заявк).{0,55}(?:ask|аск|bid|бид|лучш.{0,12}котировк).{0,30}(?:сократ|уменьш|исчез|плотност)|"
    r"(?:ask|bid).{0,30}(?:size|volume|liquidity).{0,20}(?:drop|fall|shrink|decreas|disappear)",
    re.IGNORECASE,
)
_NEGATION = re.compile(
    r"нет данных|нет подтвержден|не подтвержден|недостаточн|отсутствует|"
    r"неизвестн|не видно|не измер|не получ|не доступ|не хватает|нет полной|"
    r"без данных|невозможно подтвердить|не позволяет подтвердить",
    re.IGNORECASE,
)


def opinion_text_is_grounded(text: str, metrics: dict[str, Any]) -> bool:
    """Reject obvious generation garbage and claims unsupported by available data."""
    text = str(text or "").strip()
    if not text or _CORRUPT_MARKERS.search(text):
        return False

    lowered = text.lower()
    negated = bool(_NEGATION.search(text))
    # Even a top-quote size decrease does not establish order cancellation,
    # aggressive flow, signed delta, or an avalanche across the full book.
    if _STRONG_UNMEASURED_CLAIMS.search(text) and not negated:
        return False
    if _SIDE_SIZE_CLAIM.search(text) and not negated:
        if re.search(r"ask|аск|продажн.{0,10}заявк", lowered):
            if (_num(metrics, "ask_size_available") < 0.5
                    or int(_num(metrics, "top_ask_history_samples")) < MIN_SIZE_HISTORY_SAMPLES
                    or _num(metrics, "ask_size_drop_pct") < MIN_TOP_SIZE_DROP_PCT):
                return False
        if re.search(r"bid|бид|покупательск.{0,10}заявк", lowered):
            if (_num(metrics, "bid_size_available") < 0.5
                    or int(_num(metrics, "top_bid_history_samples")) < MIN_SIZE_HISTORY_SAMPLES
                    or _num(metrics, "bid_size_drop_pct") < MIN_TOP_SIZE_DROP_PCT):
                return False
    return True


def _complete_short_text(text: str, limit: int, fallback: str) -> str:
    """Keep a complete sentence; never cut a user-visible explanation mid-thought."""
    text = re.sub(r"\s+", " ", str(text or "")).strip(" \t\r\n,;:-…")
    if not text:
        return fallback
    match = re.search(r"^(.+?[.!?])(?:\s|$)", text)
    if match:
        sentence = match.group(1).strip()
        return sentence if len(sentence) <= limit else fallback
    if len(text) < limit:
        return text.rstrip(" ,;:-") + "."
    return fallback


def compact_fallback(decision: str, metrics: dict[str, Any], reason: str = "") -> tuple[str, str]:
    """Short deterministic explanations, always complete and grounded in measured facts."""
    decision = str(decision or "HOLD").upper()
    change = _num(metrics, "price_change_pct")
    vs_vwap = _num(metrics, "price_vs_vwap_pct")
    spike = _num(metrics, "volume_spike")

    if decision in {"BUY", "SELL"}:
        tactic = f"{decision} не подтверждён обязательными измеряемыми условиями входа."
    elif change > 0 and vs_vwap > 0:
        tactic = "Цена растёт и выше VWAP; вход ожидает подтверждения объёмом и котировкой."
    elif change < 0 and vs_vwap < 0:
        tactic = "Цена снижается и ниже VWAP; вход ожидает подтверждения объёмом и котировкой."
    else:
        tactic = "Направленный импульс не подтверждён одновременно изменением цены и VWAP."

    if reason:
        critic = reason
    elif spike < MIN_VOLUME_SPIKE:
        critic = f"Всплеск размера последней сделки {spike:.2f}× ниже порога {MIN_VOLUME_SPIKE:.1f}×."
    elif decision == "BUY":
        critic = "Не подтверждены обязательные условия по размеру лучшей котировки Ask."
    elif decision == "SELL":
        critic = "Не подтверждены обязательные условия по размеру лучшей котировки Bid."
    elif _num(metrics, "bid_ask_available") < 0.5:
        critic = "Нет актуальных Bid/Ask для проверки спреда."
    else:
        critic = "Направленный сценарий не прошёл все обязательные проверки."

    tactic = _complete_short_text(tactic, 150, "Вход не подтверждён измеряемыми условиями.")
    critic = _complete_short_text(critic, 165, "Не пройдены обязательные проверки входа.")
    return tactic, critic
