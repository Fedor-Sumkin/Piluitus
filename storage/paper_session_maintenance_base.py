"""Shared state and guards for PAPER session maintenance operations."""
from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import Any, Callable


PAPER_CLEAR_CONFIRMATION = "ОЧИСТИТЬ PAPER-СЕССИЮ"
_PAPER_MAINTENANCE_KEY = "piluitus:paper:maintenance"


class PaperSessionMaintenanceBase:
    """Own shared dependencies and fail-safe maintenance primitives."""

        def __init__(
            self,
            *,
            db_manager: Any,
            execution_engine: Any,
            risk_manager: Any,
            paper_session_lock: Any,
            anomaly_queue: Any,
            ai_candidate_deduplicator: Any,
            paper_performance_tracker: Any,
            shadow_tracker: Any,
            paper_shadow_labels: list[dict[str, Any]],
            last_ai_verdicts_cache: list[dict[str, Any]],
            trade_flow_counters: Any,
            pending_bbo_candidates: dict[str, dict[str, Any]],
            pending_bbo_lock: Any,
            position_order_book_diagnostics: Any,
            require_paper_mode: Callable[[], None],
            advance_session_generation: Callable[[], Any],
            reset_balance_guard_signature: Callable[[], Any],
            rotate_bbo_replay_session: Callable[[], Any],
            refresh_dashboard_balance_snapshot: Callable[[list[dict[str, Any]] | None], Any],
            broadcast_paper_tables: Callable[[], Any],
            broadcast_paper_session_reset: Callable[[dict[str, Any]], Any],
            get_bbo_replay_session_id: Callable[[], str],
            deposit: float,
            logger: Any,
            paper_clear_confirmation: str = PAPER_CLEAR_CONFIRMATION,
            monotonic: Callable[[], float] = time.monotonic,
            now: Callable[[], datetime] | None = None,
        ) -> None:
            self.db_manager = db_manager
            self.execution_engine = execution_engine
            self.risk_manager = risk_manager
            self.paper_session_lock = paper_session_lock
            self.anomaly_queue = anomaly_queue
            self.ai_candidate_deduplicator = ai_candidate_deduplicator
            self.paper_performance_tracker = paper_performance_tracker
            self.shadow_tracker = shadow_tracker
            self.paper_shadow_labels = paper_shadow_labels
            self.last_ai_verdicts_cache = last_ai_verdicts_cache
            self.trade_flow_counters = trade_flow_counters
            self.pending_bbo_candidates = pending_bbo_candidates
            self.pending_bbo_lock = pending_bbo_lock
            self.position_order_book_diagnostics = position_order_book_diagnostics
            self.require_paper_mode = require_paper_mode
            self.advance_session_generation = advance_session_generation
            self.reset_balance_guard_signature = reset_balance_guard_signature
            self.rotate_bbo_replay_session = rotate_bbo_replay_session
            self.refresh_dashboard_balance_snapshot = refresh_dashboard_balance_snapshot
            self.broadcast_paper_tables = broadcast_paper_tables
            self.broadcast_paper_session_reset = broadcast_paper_session_reset
            self.get_bbo_replay_session_id = get_bbo_replay_session_id
            self.deposit = float(deposit)
            self.logger = logger
            self.paper_clear_confirmation = str(paper_clear_confirmation)
            self.monotonic = monotonic
            self.now = now or (lambda: datetime.now().astimezone())

        async def _persist_maintenance(self, *, active: bool, reason: str) -> None:
            """Persist the maintenance gate before changing the corresponding phase."""
            await self.db_manager.set_json(
                _PAPER_MAINTENANCE_KEY,
                {
                    "active": bool(active),
                    "reason": str(reason),
                    "updated_at": self.now().isoformat(timespec="seconds"),
                },
            )
    
        def _purge_queued_market_events(self) -> None:
            """Drop stale market work while preserving priority-zero emergency commands."""
            emergency_items = []
            while True:
                try:
                    queued_item = self.anomaly_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                try:
                    if (
                        isinstance(queued_item, tuple)
                        and queued_item
                        and queued_item[0] == 0
                    ):
                        emergency_items.append(queued_item)
                finally:
                    self.anomaly_queue.task_done()
            for item in emergency_items:
                self.anomaly_queue.put_nowait(item)
