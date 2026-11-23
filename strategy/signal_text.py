"""Grounded Russian-language signal explanations and text validation."""
from __future__ import annotations

import re
from typing import Any

from strategy.signal_evidence import (
    MIN_SIZE_HISTORY_SAMPLES, MIN_TOP_SIZE_DROP_PCT, MIN_VOLUME_SPIKE, _num,
)


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
        critic = (
            f"Объём сделок за последние 5 секунд относительно предыдущих 55 секунд "
            f"недостаточен: {spike:.2f}× при минимуме {MIN_VOLUME_SPIKE:.1f}×."
        )
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
