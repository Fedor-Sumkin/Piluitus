"""Independent entry-quality and PAPER market-eligibility checks."""
from __future__ import annotations

import math
import os
from typing import Any

from strategy.market_context import validate_entry_economics
from strategy.signal_evidence import (
    MAX_SIZE_SIGNAL_AGE_SEC,
    MAX_SPREAD_PCT,
    MIN_SIZE_HISTORY_SAMPLES,
    MIN_TOP_SIZE_DROP_PCT,
    MIN_VOLUME_SPIKE,
    _fresh_bar_pattern_direction,
    _num,
    classify_direction_evidence,
    classify_m1_evidence_state,
    fresh_bar_pattern_direction,
    infer_candidate_direction,
    validate_atr_readiness,
)


def validate_spread_to_atr(
    metrics: dict[str, Any],
    *,
    spread_pct: float | None = None,
    atr_value: float | None = None,
    reference_price: float | None = None,
    require_atr: bool = False,
) -> tuple[bool, str]:
    """Enforce the configured spread/ATR ceiling using measured values.

    Spread and ATR are both converted to percentages. Execution callers supply
    the fresh BBO spread, captured ATR and current mid-price; pre-LLM callers use
    the market snapshot. Missing ATR is handled by the separate readiness gate.
    """
    try:
        limit = float(os.getenv("PILUITUS_MAX_SPREAD_TO_ATR_RATIO", "0.50"))
    except (TypeError, ValueError, OverflowError):
        limit = 0.50
    if not math.isfinite(limit) or limit <= 0.0:
        limit = 0.50
    limit = min(2.0, max(0.05, limit))

    try:
        spread = float(spread_pct) if spread_pct is not None else _num(metrics, "spread_pct", -1.0)
        atr = float(atr_value) if atr_value is not None else _num(metrics, "bar_atr", 0.0)
        price = float(reference_price) if reference_price is not None else _num(
            metrics, "current_price", _num(metrics, "price", 0.0)
        )
    except (TypeError, ValueError, OverflowError):
        return False, "Метрики Spread/ATR содержат некорректные значения; вход заблокирован."
    if not math.isfinite(spread) or spread < 0.0:
        return False, "Спред для проверки Spread/ATR отсутствует или некорректен; вход заблокирован."
    if not math.isfinite(atr) or atr <= 0.0:
        if require_atr:
            return False, "ATR для проверки Spread/ATR отсутствует; вход заблокирован."
        return True, "Spread/ATR будет проверен после подтверждения готовности ATR."
    atr_pct = (atr / price * 100.0) if math.isfinite(price) and price > 0.0 else _num(metrics, "bar_atr_pct", 0.0)
    if not math.isfinite(atr_pct) or atr_pct <= 0.0:
        return False, "Цена или процентный ATR для проверки Spread/ATR отсутствуют; вход заблокирован."

    if require_atr:
        try:
            min_periods = min(14, max(2, int(os.getenv("PILUITUS_ATR_MIN_PERIODS", "5"))))
        except (TypeError, ValueError, OverflowError):
            min_periods = 5
        periods = max(0, int(_num(metrics, "bar_atr_periods", 0.0)))
        if atr <= 0.0 or periods < min_periods:
            return False, f"Недостаточно истории ATR для исполнения: {periods}/{min_periods}; вход заблокирован."
        age = _num(metrics, "bar_atr_age_sec", -1.0)
        try:
            max_age = max(1.0, float(os.getenv("PILUITUS_ATR_MAX_AGE_SEC", "120")))
        except (TypeError, ValueError, OverflowError):
            max_age = 120.0
        if age < 0.0 or age > max_age:
            return False, f"История ATR устарела перед исполнением: age={age:.1f}s; лимит={max_age:.1f}s."

    ratio = spread / atr_pct
    if not math.isfinite(ratio) or ratio > limit + 1e-12:
        return False, (
            f"Spread/ATR превышает лимит: ratio={ratio:.3f}, "
            f"спред={spread:.4f}%, ATR={atr_pct:.4f}%, предел={limit:.2f}; вход заблокирован."
        )
    return True, f"Spread/ATR пройден: ratio={ratio:.3f} при лимите {limit:.2f}."


def validate_paper_sell_quality(
    metrics: dict[str, Any],
    *,
    paper_mode: bool | None = None,
) -> tuple[bool, str]:
    """Apply a deliberately stricter, PAPER-only SELL quality gate.

    The 2026-11-20 sample showed materially weaker SELL expectancy than BUY.
    This gate requires measured price/VWAP alignment and fresh negative
    aggressor flow; it never changes LIVE behaviour and can be disabled
    explicitly for controlled A/B research.
    """
    if paper_mode is None:
        paper_mode = str(os.getenv("PAPER_TRADING", "True")).strip().lower() in {
            "1", "true", "yes", "on",
        }
    enabled = str(os.getenv("PILUITUS_PAPER_SELL_STRICT_GATE", "true")).strip().lower() in {
        "1", "true", "yes", "on",
    }
    if not paper_mode or not enabled:
        return True, ""

    direction_state = classify_direction_evidence("SELL", metrics)
    if direction_state != "ALIGNED":
        return False, (
            "PAPER SELL требует полного согласования отрицательного импульса "
            "цены и VWAP; нейтральное подтверждение недостаточно."
        )

    if _num(metrics, "trade_data_ready") < 0.5:
        return False, "PAPER SELL заблокирован: поток сделок не готов для подтверждения продавцов."
    flow_age = _num(metrics, "trade_metrics_age_sec", -1.0)
    if flow_age < 0.0 or flow_age > 5.0:
        return False, (
            f"PAPER SELL заблокирован: поток сделок устарел ({flow_age:.2f}s; лимит 5.00s)."
        )
    flow = _num(metrics, "trade_flow_imbalance_30s", math.nan)
    try:
        minimum_flow = float(os.getenv("PILUITUS_PAPER_SELL_MIN_FLOW_IMBALANCE", "0.10"))
    except (TypeError, ValueError, OverflowError):
        minimum_flow = 0.10
    if not math.isfinite(minimum_flow):
        minimum_flow = 0.10
    minimum_flow = min(0.75, max(0.05, minimum_flow))
    if not math.isfinite(flow) or flow > -minimum_flow:
        flow_text = f"{flow:.3f}" if math.isfinite(flow) else "недоступен"
        return False, (
            "PAPER SELL заблокирован: недостаточно подтверждённого агрессивного потока продавцов "
            f"(imbalance_30s={flow_text}; требуется <= {-minimum_flow:.2f})."
        )
    return True, (
        f"PAPER SELL прошёл строгую проверку: цена/VWAP согласованы, "
        f"trade_flow_imbalance_30s={flow:.3f}, возраст={flow_age:.2f}s."
    )


def validate_signal_evidence(decision: str, metrics: dict[str, Any]) -> tuple[bool, str]:
    """Validate independent entry evidence; top-size depletion is supporting, not mandatory.

    Required: signed price/VWAP alignment (or a fresh matching M1 pattern), volume
    spike, coherent fresh BBO and bounded spread. Best-quote depletion is retained
    as an optional strengthening metric because a single top-level size change is
    not a reliable proxy for the full order book.
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
    spread_atr_ok, spread_atr_reason = validate_spread_to_atr(metrics)
    if not spread_atr_ok:
        return False, spread_atr_reason

    if _num(metrics, "trade_data_mode", 0.0) >= 0.5:
        if _num(metrics, "trade_data_ready", 0.0) < 0.5:
            return False, "Нет готового потока уникальных INSTRUMENT_TRADES; вход заблокирован."
        trade_age = _num(metrics, "trade_metrics_age_sec", -1.0)
        if trade_age < 0.0 or trade_age > 5.0:
            return False, f"Поток сделок устарел: age={trade_age:.2f}s; лимит 5.00s."
    volume_spike = _num(metrics, "volume_spike")
    if volume_spike < MIN_VOLUME_SPIKE:
        return False, (
            f"Объём сделок за последние 5 секунд относительно среднего 5-секундного "
            f"объёма за предыдущие 55 секунд недостаточен: {volume_spike:.2f}× "
            f"при минимуме {MIN_VOLUME_SPIKE:.1f}×."
        )

    price_change = _num(metrics, "price_change_pct")
    pattern_direction = _fresh_bar_pattern_direction(metrics)
    if pattern_direction is not None:
        if pattern_direction != decision:
            return False, (
                f"Направление свежего 1-минутного паттерна {pattern_direction} "
                f"не совпадает с решением {decision}."
            )
        direction_state = classify_direction_evidence(decision, metrics)
        if direction_state == "OPPOSITE":
            return False, (
                f"{decision} по M1-паттерну заблокирован: измеряемые данные показывают "
                "сильное встречное движение."
            )
        if direction_state == "UNAVAILABLE":
            return False, "Недостаточно направленных метрик для проверки M1-паттерна."
        direction_evidence = (
            f"свежий M1-паттерн {str(metrics.get('bar_pattern_name', '')).strip()}"
            + ("; VWAP нейтрален" if direction_state == "NEUTRAL" else "; цена/VWAP согласованы")
        )
    else:
        direction_state = classify_direction_evidence(decision, metrics)
        if direction_state == "OPPOSITE":
            return False, (
                f"{decision} заблокирован: сильное встречное подтверждение цены/VWAP "
                "против локального направления."
            )
        if direction_state == "UNAVAILABLE":
            return False, "Направленные метрики цены/VWAP недоступны; вход заблокирован."
        direction_evidence = (
            "согласованный импульс цены и VWAP"
            if direction_state == "ALIGNED"
            else "направление цены подтверждено, VWAP нейтрален; противоречия нет"
        )

    side = "ask" if decision == "BUY" else "bid"
    side_name = "Ask" if decision == "BUY" else "Bid"
    available = _num(metrics, f"{side}_size_available") >= 0.5
    samples = int(_num(metrics, f"top_{side}_history_samples"))
    drop_pct = _num(metrics, f"{side}_size_drop_pct")
    drop_age = _num(metrics, f"{side}_size_drop_age_sec", -1.0)
    size_confirmed = (
        available
        and samples >= MIN_SIZE_HISTORY_SAMPLES
        and drop_pct >= MIN_TOP_SIZE_DROP_PCT
        and 0.0 <= drop_age <= MAX_SIZE_SIGNAL_AGE_SEC
    )
    if size_confirmed:
        size_evidence = f"свежая разгрузка лучшего {side_name} {drop_pct:.1f}%"
    else:
        baseline = _num(metrics, f"{side}_size_baseline")
        current_size = _num(metrics, f"{side}_size_current")
        observed = (
            max(0.0, min(100.0, (baseline - current_size) / baseline * 100.0))
            if baseline > 0.0 and current_size >= 0.0 else None
        )
        observed_text = (
            f"наблюдаемое изменение размера {observed:.1f}%"
            if observed is not None else "наблюдение размера отсутствует"
        )
        size_evidence = (
            f"разгрузка лучшего {side_name} не подтверждена "
            f"(подтверждено={drop_pct:.1f}%, {observed_text}, история={samples}, "
            f"возраст={drop_age:.2f}s); размер — дополнительный, не обязательный признак"
        )

    return True, (
        f"Обязательные условия пройдены: {direction_evidence}, volume_spike={volume_spike:.2f}×, "
        f"свежий BBO и спред={spread_pct:.3f}%; {size_evidence}."
    )



def validate_paper_market_quality(
    decision: str,
    metrics: dict[str, Any],
) -> tuple[bool, str]:
    """Require a fresh confirmed book and positive round-trip cost/ATR edge in PAPER."""
    side = str(decision or "").strip().upper()
    if side not in {"BUY", "SELL"} or not isinstance(metrics, dict):
        return False, "PAPER entry quality requires a BUY/SELL candidate and measured metrics."

    if _num(metrics, "order_book_valid") < 0.5:
        return False, "PAPER-вход заблокирован: актуальный ORDER_BOOK не подтверждён."

    book_age = _num(metrics, "order_book_age_sec", -1.0)
    max_age = min(5.0, max(0.25, _num(metrics, "bbo_max_age_sec", 2.0)))
    if book_age < 0.0 or book_age > max_age:
        age_text = "неизвестен" if book_age < 0.0 else f"{book_age:.2f}s"
        return False, (
            f"PAPER-вход заблокирован: стакан устарел (age={age_text}; "
            f"лимит={max_age:.2f}s)."
        )

    bid = _num(metrics, "bid", math.nan)
    ask = _num(metrics, "ask", math.nan)
    atr_pct = _num(metrics, "bar_atr_pct", math.nan)
    try:
        commission = float(os.getenv("PILUITUS_PAPER_COMMISSION_PCT", "0.0354"))
        slippage = float(os.getenv("PILUITUS_PAPER_SLIPPAGE_BPS", "0"))
        max_cost_ratio = float(os.getenv("PILUITUS_MAX_ENTRY_COST_TO_ATR_RATIO", "0.50"))
    except (TypeError, ValueError, OverflowError):
        return False, "PAPER-вход заблокирован: параметры модели издержек некорректны."

    economics = validate_entry_economics(
        side=side,
        bid=bid,
        ask=ask,
        commission_pct_per_side=commission,
        slippage_bps_per_side=slippage,
        atr_pct=atr_pct,
        max_cost_to_atr_ratio=max_cost_ratio,
    )
    if economics.get("available"):
        metrics["entry_cost_to_atr_ratio"] = economics.get("cost_to_atr_ratio")
        metrics["entry_minimum_favorable_move_pct"] = economics.get("minimum_favorable_move_pct")
        metrics["entry_cost_to_atr_limit"] = economics.get("max_cost_to_atr_ratio")
        metrics["entry_cost_model_status"] = economics.get("cost_model_status")
    if not economics.get("available") or not economics.get("eligible"):
        ratio = economics.get("cost_to_atr_ratio")
        ratio_text = f"{float(ratio):.3f}" if isinstance(ratio, (int, float)) and math.isfinite(float(ratio)) else "н/д"
        return False, (
            "PAPER-вход заблокирован: round-trip издержки слишком велики или не измерены "
            f"(cost/ATR={ratio_text}, лимит={max_cost_ratio:.2f}, "
            f"причина={economics.get('reason', 'UNKNOWN')})."
        )

    return True, (
        "PAPER market quality подтверждена: свежий ORDER_BOOK; "
        f"cost/ATR={float(economics['cost_to_atr_ratio']):.3f} "
        f"<= {float(economics['max_cost_to_atr_ratio']):.2f}; "
        f"минимальное движение до безубыточности="
        f"{float(economics['minimum_favorable_move_pct']):.4f}%; "
        f"модель издержек={economics.get('cost_model_status', 'UNKNOWN')}."
    )

def validate_entry_evidence(
    decision: str,
    metrics: dict[str, Any],
    *,
    paper_mode: bool = False,
) -> tuple[bool, str]:
    """Validate common evidence, then enforce PAPER-only market-quality and SELL gates.

    The base signal validator remains suitable for diagnostics and unit tests.
    PAPER-specific checks run only on the actual PAPER entry path.
    """
    ok, reason = validate_signal_evidence(decision, metrics)
    if not ok:
        return ok, reason
    if paper_mode:
        m1_gate_enabled = str(
            os.getenv("PILUITUS_PAPER_M1_OPPOSITE_GATE", "true")
        ).strip().lower() in {"1", "true", "yes", "on"}
        if m1_gate_enabled:
            m1_state = classify_m1_evidence_state(metrics, decision)
            if m1_state == "OPPOSITE":
                return False, (
                    "PAPER-вход заблокирован: подтверждённый закрытыми M1-свечами "
                    f"тренд/паттерн противоречит направлению {str(decision).upper()}."
                )
        market_ok, market_reason = validate_paper_market_quality(decision, metrics)
        if not market_ok:
            return False, market_reason
    if str(decision or "").strip().upper() == "SELL" and paper_mode:
        sell_ok, sell_reason = validate_paper_sell_quality(metrics, paper_mode=True)
        if not sell_ok:
            return False, sell_reason
        reason = f"{reason} {sell_reason}".strip()
    if paper_mode:
        reason = f"{reason} {market_reason}".strip()
    return True, reason
