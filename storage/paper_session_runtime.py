"""Persistence and session rotation services for PAPER runtime state."""
from __future__ import annotations

from typing import Any


class PaperSessionRuntime:
    """Own persistence, migration and BBO replay rotation for a PAPER session."""

    def __init__(
        self,
        db_manager: Any,
        execution_engine: Any,
        shadow_outcome_persistence: Any,
        bbo_replay_recorder: Any,
        bbo_replay_recorder_factory: Any,
        paper_performance_tracker: Any,
        paper_shadow_labels: list[dict[str, Any]],
        logger: Any,
        *,
        closed_trades_storage_key: str,
        closed_trades_storage_limit: int,
    ) -> None:
        self.db_manager = db_manager
        self.execution_engine = execution_engine
        self.shadow_outcome_persistence = shadow_outcome_persistence
        self.bbo_replay_recorder = bbo_replay_recorder
        self.bbo_replay_recorder_factory = bbo_replay_recorder_factory
        self.paper_performance_tracker = paper_performance_tracker
        self.paper_shadow_labels = paper_shadow_labels
        self.logger = logger
        self.closed_trades_storage_key = closed_trades_storage_key
        self.closed_trades_storage_limit = int(closed_trades_storage_limit)

    async def persist_shadow_outcomes(
        self, labels: list[dict[str, Any]], *, source: str
    ) -> None:
        await self.shadow_outcome_persistence.persist_completed_labels(
            labels,
            session_id=str(
                getattr(self.execution_engine, "session_id", "unknown")
            ),
            source=source,
        )

    async def restore_shadow_outcomes(self) -> None:
        await self.shadow_outcome_persistence.restore_completed_labels(
            paper_mode=bool(
                getattr(self.execution_engine, "paper_mode", True)
            ),
            session_id=getattr(self.execution_engine, "session_id", "unknown"),
        )

    async def rotate_bbo_replay_session(self) -> Any:
        """Drain the old replay writer and start a clean recorder session."""
        previous = self.bbo_replay_recorder
        await previous.close()
        recorder = self.bbo_replay_recorder_factory()
        self.bbo_replay_recorder = recorder
        self.execution_engine.bbo_replay_session_id = recorder.session_id
        self.execution_engine.paper_exit_event_recorder = recorder.record_exit_trigger
        recorder.start(self.db_manager)
        self.logger.info(
            "[PAPER BBO REPLAY SESSION RESET] session_id=%s previous_session_id=%s",
            recorder.session_id, previous.session_id,
        )
        return recorder

    async def restore_paper_performance(self) -> None:
        """Restore realized PAPER trades; migrate the old short history if needed."""
        try:
            stored = await self.db_manager.get_list(
                self.closed_trades_storage_key,
                start=0,
                stop=self.closed_trades_storage_limit - 1,
            )
            source = "dedicated_journal"
            if not stored:
                stored = await self.db_manager.get_list(
                    "piluitus:trade_history", start=0, stop=99
                )
                source = "legacy_trade_history"
            restored = self.paper_performance_tracker.restore(stored)
            if (
                source == "legacy_trade_history"
                and self.paper_performance_tracker.records
            ):
                try:
                    await self.db_manager.push_many_to_list(
                        self.closed_trades_storage_key,
                        self.paper_performance_tracker.records,
                        max_len=self.closed_trades_storage_limit,
                    )
                    self.logger.info(
                        "[PAPER PERFORMANCE MIGRATION] legacy_rows=%d copied_to_dedicated_journal=true",
                        len(self.paper_performance_tracker.records),
                    )
                except Exception as migration_exc:
                    self.logger.error(
                        "[PAPER PERFORMANCE MIGRATION ERROR] legacy_rows=%d error=%s",
                        len(self.paper_performance_tracker.records),
                        migration_exc, exc_info=True,
                    )
            summary = self.paper_performance_tracker.scorecard(
                self.paper_shadow_labels
            )
            overall = summary["overall"]
            self.logger.info(
                "[PAPER PERFORMANCE RESTORE] source=%s loaded=%d restored=%d total=%d "
                "net=%+.2f RUB win_rate=%s PF=%s max_DD=%.2f RUB",
                source, len(stored), restored, overall["trades"],
                overall["net_pnl_rub"],
                (
                    f'{overall["win_rate_pct"]:.1f}%'
                    if overall["win_rate_pct"] is not None else "N/A"
                ),
                (
                    f'{overall["profit_factor"]:.3f}'
                    if overall["profit_factor"] is not None else "N/A"
                ),
                overall["max_drawdown_rub"],
            )
        except Exception as exc:
            self.logger.error(
                "[PAPER PERFORMANCE RESTORE ERROR] %s", exc, exc_info=True
            )
