"""Candidate-local measurements and pre-LLM RiskManager admission."""
from __future__ import annotations

from typing import Any

from runtime.market_worker_candidate_state import MarketWorkerCandidateState


class MarketWorkerCandidateAdmissionMixin:
    """Prepare a candidate and stop it before LLM when local risk rejects it."""

    async def _prepare_market_candidate_state(self, event_payload: dict[str, Any]) -> MarketWorkerCandidateState | None:
        ticker   = event_payload.get("ticker") or event_payload.get("security_code") or "UNKNOWN"
        price    = event_payload.get("price") or 0.0
        metrics  = event_payload.get("metrics", {})
        if not isinstance(metrics, dict):
            metrics = {}
        candidate_direction_for_diag = str(event_payload.get("candidate_direction") or "").upper()
        llm_input_features = self.build_llm_feature_snapshot(metrics)
        volume_spike = self.parse_finite_float(metrics.get("volume_spike"))
        volume_5s = self.parse_finite_float(metrics.get("trade_volume_5s"))
        volume_baseline_5s = self.parse_finite_float(metrics.get("trade_volume_baseline_5s"))
        trade_count_60s = self.parse_finite_float(metrics.get("trade_count_60s"))
        trade_data_ready = (self.parse_finite_float(metrics.get("trade_data_ready")) or 0.0) >= 0.5
        volume_spike_text = f"{volume_spike:.2f}×" if volume_spike is not None else "Н/Д"
        if trade_data_ready and volume_5s is not None and volume_baseline_5s is not None:
            baseline_buckets = int(self.parse_finite_float(metrics.get("trade_baseline_bucket_count")) or 0)
            trades_5s = int(self.parse_finite_float(metrics.get("trade_count_5s")) or 0)
            volume_text = (
                f"Всплеск объёма: {volume_spike_text}; "
                f"5с={volume_5s:.3f} ед. провайдера, медианная база={volume_baseline_5s:.3f}; "
                f"сделок 5с={trades_5s}, сделок 60с={int(trade_count_60s or 0)}, "
                f"активных 5с-интервалов базы={baseline_buckets}."
            )
        else:
            volume_text = f"Объём сделок: Н/Д — история потока не готова; spike={volume_spike_text}."

        trade_realized_volatility = self.parse_finite_float(
            metrics.get("trade_realized_volatility_60s_pct")
        )
        m1_atr_pct = self.parse_finite_float(metrics.get("bar_atr_pct"))
        m1_atr_periods = self.parse_finite_float(metrics.get("bar_atr_periods")) or 0.0
        if trade_data_ready and trade_realized_volatility is not None:
            volatility_text = (
                f"Реализованная волатильность по сделкам за 60с: "
                f"{trade_realized_volatility:.5f}%."
            )
        elif m1_atr_pct is not None and m1_atr_pct > 0.0 and m1_atr_periods >= 5.0:
            volatility_text = f"M1 ATR ({int(m1_atr_periods)} закрытых баров): {m1_atr_pct:.5f}%."
        else:
            volatility_text = "Волатильность: Н/Д — нет подтверждённой истории."

        self.background_operations.spawn(
            self.log_trading_event(
                ticker=ticker,
                price=price,
                decision="В ОЧЕРЕДИ (ПРОВЕРКА РИСКОВ)",
                confidence=0.0,
                tactic=volume_text,
                critic=volatility_text,
                candidate_id=str(event_payload.get("shadow_candidate_id") or "") or None,
            ),
            name=f"trading-log-queue:{ticker}",
        )

        trading_allowed = await self.risk_manager.is_trading_allowed(self.db)
        if (
            getattr(execution_engine, "_maintenance_active", False)
            or event_payload.get("paper_session_generation") != self.session_generation_provider()
        ):
            return
        if not trading_allowed:
            self.trade_flow_increment("risk_blocked")
            self.background_operations.spawn(
                self.log_trading_event(
                    ticker,
                    price,
                    "RISK_BLOCK",
                    0.0,
                    "Торговля заблокирована.",
                    "RiskManager запретил торговлю до запроса к ИИ.",
                    candidate_id=str(event_payload.get("shadow_candidate_id") or "") or None,
                ),
                name=f"trading-log-risk-block:{ticker}",
            )
            await self.publish_pre_llm_risk_block(
                event_payload, int(event_payload.get("paper_session_generation", -1))
            )
            return
        return {
            "event_payload": event_payload,
            "ticker": ticker,
            "price": price,
            "metrics": metrics,
            "candidate_direction_for_diag": candidate_direction_for_diag,
            "llm_input_features": llm_input_features,
        }
