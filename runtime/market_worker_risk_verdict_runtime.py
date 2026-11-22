"""Session-safe publication of local pre-LLM RiskManager verdicts."""
from __future__ import annotations

import json
from typing import Any


class MarketWorkerRiskVerdictMixin:
    """Publish rate-limited local risk blocks without invoking the LLM."""

    async def publish_pre_llm_risk_block(
        self, event_payload: dict[str, Any], session_generation: int
    ) -> None:
        """Publish a local pre-LLM risk rejection only while its session is current."""
        async with self.session_lock:
            if (
                getattr(self.execution_engine, "_maintenance_active", False)
                or session_generation != self.session_generation_provider()
            ):
                return
            await self.publish_pre_llm_risk_block_locked(event_payload)

    async def publish_pre_llm_risk_block_locked(
        self, event_payload: dict[str, Any]
    ) -> None:
        """Persist and broadcast a rate-limited RiskManager verdict."""
        now_mono = self.monotonic()
        if now_mono - self._last_risk_block_verdict_at < 10.0:
            return
        self._last_risk_block_verdict_at = now_mono
        verdict = self.build_pre_llm_risk_block_verdict(
            event_payload,
            now_local=self.now_local(),
            verdict_id=self.uuid_factory().hex,
            explanation_builder=self.build_verdict_explanation,
        )
        ticker = verdict["ticker"]
        self.verdict_cache.append(verdict)
        if len(self.verdict_cache) > self.max_cache_size:
            self.verdict_cache.pop(0)
        try:
            redis_inst = getattr(self.db, "redis", getattr(self.db, "_redis", None))
            if redis_inst is not None:
                await redis_inst.lpush(
                    "piluitus:verdicts:history",
                    json.dumps(verdict, ensure_ascii=False),
                )
                await redis_inst.ltrim(
                    "piluitus:verdicts:history", 0, self.max_cache_size - 1
                )
        except Exception as persist_exc:
            self.logger.warning(
                "Не удалось сохранить RiskManager-вердикт %s: %s",
                ticker,
                persist_exc,
            )
        try:
            payload = {
                "type": "MARKET_UPDATE",
                **verdict,
                "balance": dict(self.balance_snapshot),
            }
            await self.manager.broadcast(json.dumps(payload, ensure_ascii=False))
        except Exception as broadcast_exc:
            self.logger.error(
                "Не удалось отправить RiskManager-вердикт в веб-интерфейс: %s",
                broadcast_exc,
            )
