"""Publish verdicts under the PAPER-session lock and schedule execution."""
from __future__ import annotations

from runtime.market_worker_candidate_state import MarketWorkerCandidateState


class MarketWorkerVerdictPublicationMixin:
    """Publish only current-session verdicts and queue approved executions."""

    async def _publish_candidate_verdict(self, state: MarketWorkerCandidateState) -> None:
        event_payload = state["event_payload"]
        ticker = state["ticker"]
        price = state["price"]
        ai_analysis = state["ai_analysis"]
        verdict_data = state["verdict_data"]
        execution_status = state["execution_status"]
        requested_lots = state["requested_lots"]
        decision = state["decision"]
        shadow_candidate_id = state["shadow_candidate_id"]
        verdict_id = state["verdict_id"]
        execution_evidence_at_monotonic = state.get("execution_evidence_at_monotonic")
        async with self.paper_session_generation_lock:
            if (
                getattr(self.execution_engine, "_maintenance_active", False)
                or event_payload.get("paper_session_generation") != self.session_generation_provider()
            ):
                return
            await self.publish_market_verdict(
                verdict_data,
                db=self.db,
                verdict_cache=self.verdict_cache,
                max_cache_size=self.max_cache_size,
                manager=self.manager,
                balance_snapshot=self.balance_snapshot,
                logger=self.logger,
                candidate_id=str(event_payload.get("shadow_candidate_id") or "NONE"),
            )

            if execution_status == "PENDING":
                self.background_operations.spawn(
                    self.execute_and_publish(
                        verdict_id=verdict_id,
                        ticker=ticker,
                        lots=requested_lots,
                        side=decision,
                        price=price,
                        ai_analysis=dict(ai_analysis),
                        session_generation=int(event_payload.get("paper_session_generation", -1)),
                        # Queue age was checked before LLM; execution TTL starts from the
                        # fresh BBO/trade snapshot that passed the post-LLM gates.
                        candidate_created_monotonic=execution_evidence_at_monotonic,
                        shadow_candidate_id=shadow_candidate_id or None,
                    ),
                    name=f"paper-execution:{ticker}:{verdict_id}",
                )
