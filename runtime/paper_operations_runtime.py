"""Composition root for PAPER operator and research APIs.

The storage runtimes retain their focused responsibilities. This module wires
their HTTP-facing adapters, maintenance workflow and route registration.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from interface.paper_api_helpers import PaperAPIHelpers
from interface.paper_research_routes import create_paper_research_router
from interface.paper_session_routes import create_paper_session_router
from storage.paper_research_runtime import PaperResearchRuntime
from storage.paper_session_maintenance_runtime import (
    PAPER_CLEAR_CONFIRMATION,
    PaperSessionMaintenanceRuntime,
)


class PaperOperationsRuntime:
    """Compose PAPER API helpers, research services and maintenance routes."""

    def __init__(
        self,
        *,
        dependencies: Mapping[str, Any],
        callbacks: Mapping[str, Any],
    ) -> None:
        self.dependencies = dict(dependencies)
        self.callbacks = dict(callbacks)
        self.paper_api_helpers: Any | None = None
        self.paper_research_runtime: Any | None = None
        self.paper_session_maintenance_runtime: Any | None = None

    def _require_created(self) -> None:
        if self.paper_api_helpers is None:
            raise RuntimeError("PaperOperationsRuntime.create() must run first.")

    def create(self, app: Any) -> "PaperOperationsRuntime":
        """Build the PAPER API graph and register its route groups."""
        deps = self.dependencies
        callbacks = self.callbacks

        self.paper_api_helpers = PaperAPIHelpers(
            execution_engine=deps["execution_engine"],
            connection_manager=deps["connection_manager"],
            get_open_positions=deps["get_open_positions"],
            refresh_balance=deps["refresh_balance"],
        )

        self.paper_research_runtime = PaperResearchRuntime(
            db_manager=deps["db_manager"],
            is_paper_trade=deps["is_paper_trade"],
            execution_engine=deps["execution_engine"],
            screener=deps["screener"],
            paper_performance_tracker=deps["paper_performance_tracker"],
            paper_shadow_labels=deps["paper_shadow_labels"],
            paper_session_lock=deps["paper_session_lock"],
            broadcast_paper_tables=self.broadcast_paper_tables,
            bbo_replay_storage_key=deps["bbo_replay_storage_key"],
            bbo_replay_storage_limit=deps["bbo_replay_storage_limit"],
            paper_closed_trades_storage_limit=deps[
                "paper_closed_trades_storage_limit"
            ],
            shadow_outcomes_storage_limit=deps["shadow_outcomes_storage_limit"],
            lot_sizes=deps["lot_sizes"],
            parse_finite_float=deps["parse_finite_float"],
            replay_exit_batch=deps["replay_exit_batch"],
            replay_exit_scenarios=deps["replay_exit_scenarios"],
            build_shadow_reconciliation=deps["build_shadow_reconciliation"],
            build_actual_vs_replay_reconciliation=deps[
                "build_actual_vs_replay_reconciliation"
            ],
            build_shadow_cost_sensitivity=deps[
                "build_shadow_cost_sensitivity"
            ],
            build_shadow_walk_forward_validation=deps[
                "build_shadow_walk_forward_validation"
            ],
        )

        self.paper_session_maintenance_runtime = PaperSessionMaintenanceRuntime(
            db_manager=deps["db_manager"],
            execution_engine=deps["execution_engine"],
            risk_manager=deps["risk_manager"],
            paper_session_lock=deps["paper_session_lock"],
            anomaly_queue=deps["anomaly_queue"],
            ai_candidate_deduplicator=deps["ai_candidate_deduplicator"],
            paper_performance_tracker=deps["paper_performance_tracker"],
            shadow_tracker=deps["shadow_tracker"],
            paper_shadow_labels=deps["paper_shadow_labels"],
            last_ai_verdicts_cache=deps["last_ai_verdicts_cache"],
            trade_flow_counters=deps["trade_flow_counters"],
            pending_bbo_candidates=deps["pending_bbo_candidates"],
            pending_bbo_lock=deps["pending_bbo_lock"],
            position_order_book_diagnostics=deps[
                "position_order_book_diagnostics"
            ],
            require_paper_mode=self.require_paper_mode,
            advance_session_generation=callbacks[
                "advance_session_generation"
            ],
            reset_balance_guard_signature=callbacks[
                "reset_balance_guard_signature"
            ],
            rotate_bbo_replay_session=callbacks[
                "rotate_bbo_replay_session"
            ],
            refresh_dashboard_balance_snapshot=deps["refresh_balance"],
            broadcast_paper_tables=self.broadcast_paper_tables,
            broadcast_paper_session_reset=self.broadcast_paper_session_reset,
            get_bbo_replay_session_id=callbacks["get_bbo_replay_session_id"],
            deposit=deps["deposit"],
            logger=deps["logger"],
            paper_clear_confirmation=deps.get(
                "paper_clear_confirmation", PAPER_CLEAR_CONFIRMATION
            ),
            monotonic=deps["monotonic"],
        )

        app.include_router(
            create_paper_research_router(self.paper_research_runtime)
        )
        app.include_router(
            create_paper_session_router(
                self.paper_session_maintenance_runtime
            )
        )
        return self

    def require_paper_mode(self) -> None:
        self._require_created()
        self.paper_api_helpers.require_paper_mode()

    def paper_trade_rows(self, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        self._require_created()
        return self.paper_api_helpers.paper_trade_rows(snapshot)

    async def broadcast_paper_tables(self) -> None:
        self._require_created()
        await self.paper_api_helpers.broadcast_paper_tables()

    async def broadcast_paper_session_reset(
        self, payload: dict[str, Any]
    ) -> None:
        manager = self.dependencies["connection_manager"]
        await manager.broadcast(json.dumps(payload, ensure_ascii=False))
