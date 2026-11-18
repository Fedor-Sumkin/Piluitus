"""Realized PAPER trade journal analytics.

Only final CLOSED trade records are accepted. Shadow labels are joined by the
stable candidate_id carried on the trade; ticker/time proximity is never used.
"""
from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Mapping

PAPER_CLOSED_TRADES_STORAGE_KEY = "piluitus:paper:closed_trades"
PAPER_CLOSED_TRADES_STORAGE_LIMIT = 20000


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _text(value: object, default: str = "UNKNOWN") -> str:
    text = str(value or "").strip()
    return text[:120] if text else default


def _confidence_bucket(value: object) -> str:
    confidence = _number(value)
    if confidence is None or not 0.0 <= confidence <= 100.0:
        return "UNKNOWN"
    lower = int(confidence // 10) * 10
    upper = min(100, lower + 9)
    return f"{lower:02d}-{upper:02d}"


def _metrics(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    pnls = [_number(row.get("pnl")) for row in rows]
    valid = [value for value in pnls if value is not None]
    wins = [value for value in valid if value > 0.0]
    losses = [value for value in valid if value < 0.0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    max_drawdown_pct: float | None = None
    ordered = sorted(
        (row for row in rows if _number(row.get("pnl")) is not None),
        key=lambda row: _text(row.get("exit_timestamp"), ""),
    )
    for row in ordered:
        equity += float(_number(row.get("pnl")) or 0.0)
        peak = max(peak, equity)
        drawdown = peak - equity
        if drawdown > max_drawdown:
            max_drawdown = drawdown
            max_drawdown_pct = (drawdown / peak * 100.0) if peak > 0.0 else None

    holding = [
        value for row in rows
        if (value := _number(row.get("time_in_trade_sec"))) is not None and value >= 0.0
    ]
    commissions = [
        value for row in rows
        if (value := _number(row.get("total_commission_rub"))) is not None and value >= 0.0
    ]
    return {
        "trades": len(valid),
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": len(valid) - len(wins) - len(losses),
        "win_rate_pct": round(len(wins) / len(valid) * 100.0, 3) if valid else None,
        "net_pnl_rub": round(sum(valid), 2),
        "gross_profit_rub": round(gross_profit, 2),
        "gross_loss_rub": round(gross_loss, 2),
        "expectancy_rub": round(sum(valid) / len(valid), 2) if valid else None,
        "average_win_rub": round(sum(wins) / len(wins), 2) if wins else None,
        "average_loss_rub": round(sum(losses) / len(losses), 2) if losses else None,
        "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0.0 else None,
        "max_drawdown_rub": round(max_drawdown, 2),
        "max_drawdown_pct_of_prior_peak": (
            round(max_drawdown_pct, 3) if max_drawdown_pct is not None else None
        ),
        "average_holding_sec": round(sum(holding) / len(holding), 2) if holding else None,
        "total_commission_rub": round(sum(commissions), 2),
    }


class PaperPerformanceTracker:
    """Bounded, idempotent in-memory view of the durable closed-trade journal."""

    def __init__(self, max_records: int = PAPER_CLOSED_TRADES_STORAGE_LIMIT) -> None:
        self.max_records = max(1, int(max_records))
        self._by_id: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _valid_trade(trade: object) -> dict[str, Any] | None:
        if not isinstance(trade, Mapping):
            return None
        if str(trade.get("status") or "").upper() != "CLOSED":
            return None
        trade_id = _text(trade.get("id"), "")
        pnl = _number(trade.get("pnl"))
        side = str(trade.get("side") or "").upper()
        if not trade_id or pnl is None or side not in {"BUY", "SELL"}:
            return None
        row = dict(trade)
        row["id"] = trade_id
        row["side"] = side
        row["ticker"] = _text(trade.get("ticker"))
        row["pnl"] = round(pnl, 2)
        return row

    def record(self, trade: object) -> bool:
        row = self._valid_trade(trade)
        if row is None or row["id"] in self._by_id:
            return False
        self._by_id[row["id"]] = row
        while len(self._by_id) > self.max_records:
            oldest_id = next(iter(self._by_id))
            self._by_id.pop(oldest_id, None)
        return True

    def restore(self, trades: object) -> int:
        if not isinstance(trades, (list, tuple)):
            return 0
        restored = 0
        # The storage list is newest-first; replay oldest-first for stable eviction.
        for trade in reversed(trades):
            restored += int(self.record(trade))
        return restored

    @property
    def records(self) -> list[dict[str, Any]]:
        return list(self._by_id.values())

    @staticmethod
    def _group(rows: list[dict[str, Any]], field: str, transform=None) -> dict[str, Any]:
        buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            raw = row.get(field)
            key = transform(raw) if transform else _text(raw)
            buckets[str(key)].append(row)
        return {
            key: _metrics(sorted(
                bucket,
                key=lambda item: _text(item.get("exit_timestamp"), ""),
            ))
            for key, bucket in sorted(buckets.items())
        }

    def scorecard(self, shadow_labels: object = None) -> dict[str, Any]:
        rows = sorted(
            self.records,
            key=lambda row: _text(row.get("exit_timestamp"), ""),
        )
        result: dict[str, Any] = {
            "sample_quality": (
                "INSUFFICIENT_LT_30" if len(rows) < 30
                else "PRELIMINARY_30_99" if len(rows) < 100
                else "DEVELOPING_100_299" if len(rows) < 300
                else "LARGE_300_PLUS"
            ),
            "overall": _metrics(rows),
            "by_side": self._group(rows, "side"),
            "by_ticker": self._group(rows, "ticker"),
            "by_llm_model_decision": self._group(rows, "llm_model_decision"),
            "by_llm_final_decision": self._group(rows, "llm_final_decision"),
            "by_confidence": self._group(rows, "llm_confidence_pct", _confidence_bucket),
            "by_llm_reason": self._group(rows, "llm_reason_code"),
            "by_exit_reason": self._group(rows, "exit_reason"),
        }
        result["shadow_comparison"] = self._compare_shadow(rows, shadow_labels)
        return result

    @staticmethod
    def _compare_shadow(
        trades: list[dict[str, Any]], labels: object
    ) -> dict[str, Any]:
        label_rows = labels if isinstance(labels, (list, tuple)) else []
        by_candidate: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
        for label in label_rows:
            if not isinstance(label, Mapping) or str(label.get("result") or "") != "OK":
                continue
            candidate_id = _text(label.get("candidate_id"), "")
            horizon = _number(label.get("horizon_sec"))
            net_return = _number(label.get("net_executable_return_pct"))
            if not candidate_id or horizon is None or net_return is None or horizon <= 0:
                continue
            # A candidate has at most one observation per horizon; ignore duplicate
            # persisted rows rather than inflating the matched sample.
            by_candidate[candidate_id].setdefault(int(horizon), dict(label))

        matched: list[dict[str, Any]] = []
        unmatched = 0
        horizon_returns: dict[int, list[float]] = defaultdict(list)
        for trade in trades:
            candidate_id = _text(trade.get("shadow_candidate_id"), "")
            candidate_labels = by_candidate.get(candidate_id) if candidate_id else None
            if not candidate_labels:
                unmatched += 1
                continue
            matched.append(trade)
            for horizon, label in candidate_labels.items():
                value = _number(label.get("net_executable_return_pct"))
                if value is not None:
                    horizon_returns[horizon].append(value)

        horizons = {}
        for horizon, values in sorted(horizon_returns.items()):
            positive = [value for value in values if value > 0.0]
            negative = [value for value in values if value < 0.0]
            horizons[str(horizon)] = {
                "labels": len(values),
                "mean_net_return_pct": round(sum(values) / len(values), 6),
                "positive_rate_pct": round(len(positive) / len(values) * 100.0, 3),
                "profit_factor": (
                    round(sum(positive) / abs(sum(negative)), 4)
                    if sum(negative) < 0.0 else None
                ),
            }
        return {
            "matched_trades": len(matched),
            "unmatched_trades": unmatched,
            "join_key": "shadow_candidate_id",
            "horizons": horizons,
        }


paper_performance_tracker = PaperPerformanceTracker()
