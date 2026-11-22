"""Top-level bootstrap for the complete PILUITUS application runtime graph."""
from __future__ import annotations

import os
import time
from collections.abc import Callable, Mapping
from typing import Any

from config import FINAM_TRADE_LOT_SIZES, LOT_SIZES, TICKERS_TO_TRADE
from runtime.application_composition_root_runtime import ApplicationCompositionRootRuntime
from runtime.application_core_runtime import ApplicationCoreRuntime
from runtime.application_shared_state_runtime import ApplicationSharedStateRuntime
from runtime.market_runtime_composition_runtime import MarketRuntimeCompositionRuntime
from runtime.paper_session_composition_runtime import PaperSessionCompositionRuntime
from storage.shadow_outcomes import SHADOW_OUTCOMES_STORAGE_LIMIT
from strategy.bbo_replay import BBOReplayRecorder
from strategy.paper_performance import (
    PAPER_CLOSED_TRADES_STORAGE_KEY,
    PAPER_CLOSED_TRADES_STORAGE_LIMIT,
    paper_performance_tracker,
)


class ApplicationRuntimeBootstrap:
    """Compose Core, shared state, PAPER session, Market and FastAPI once."""

    def __init__(
        self,
        *,
        settings: Any,
        finam_token: str,
        client_id: str,
        deposit: float,
        logger: Any,
        terminal_status: Any,
        environment: Mapping[str, str] | None = None,
        tickers: list[str] | tuple[str, ...] = TICKERS_TO_TRADE,
        finam_trade_lot_sizes: Mapping[str, Any] = FINAM_TRADE_LOT_SIZES,
        lot_sizes: Mapping[str, Any] = LOT_SIZES,
        verdict_cache: list[dict[str, Any]] | None = None,
        max_cache_size: int = 100,
        monotonic: Callable[[], float] = time.monotonic,
        set_balance_snapshot: Callable[[dict[str, Any]], None] | None = None,
        set_bbo_replay_recorder: Callable[[Any], None] | None = None,
        set_verdict_cache: Callable[[list[dict[str, Any]]], None] | None = None,
        set_market_task: Callable[[Any], None] | None = None,
        set_session_generation: Callable[[int], None] | None = None,
    ) -> None:
        self.settings = settings
        self.finam_token = str(finam_token or "")
        self.client_id = str(client_id or "UNKNOWN_ACCOUNT")
        self.deposit = float(deposit)
        self.logger = logger
        self.terminal_status = terminal_status
        self.environment = environment if environment is not None else os.environ
        # Keep the original configuration objects and their identity intact.
        self.tickers = tickers
        self.finam_trade_lot_sizes = finam_trade_lot_sizes
        self.lot_sizes = lot_sizes
        self.verdict_cache = verdict_cache if verdict_cache is not None else []
        self.max_cache_size = max(1, int(max_cache_size))
        self.monotonic = monotonic
        self._publish_balance_snapshot_callback = set_balance_snapshot or (lambda value: None)
        self._publish_recorder_callback = set_bbo_replay_recorder or (lambda value: None)
        self._publish_verdict_cache_callback = set_verdict_cache or (lambda values: None)
        self._publish_market_task_callback = set_market_task or (lambda task: None)
        self._publish_generation_callback = set_session_generation or (lambda value: None)

        self.dashboard_balance_snapshot: dict[str, Any] | None = None
        self.paper_session_generation = 0
        self.application_core_runtime: Any | None = None
        self.application_shared_state_runtime: Any | None = None
        self.paper_session_composition_runtime: Any | None = None
        self.market_runtime_composition: Any | None = None
        self.application_composition_root_runtime: Any | None = None
        self._creation_started = False
        self._created = False

    def _set_balance_snapshot(self, snapshot: dict[str, Any]) -> None:
        self.dashboard_balance_snapshot = snapshot
        self._publish_balance_snapshot_callback(snapshot)

    def _require_shared_state(self) -> Any:
        if self.application_shared_state_runtime is None:
            raise RuntimeError("Application shared state is unavailable before bootstrap creation.")
        return self.application_shared_state_runtime

    def set_bbo_replay_recorder(self, recorder: Any) -> None:
        self._require_shared_state().set_bbo_replay_recorder(recorder)
        self._publish_recorder_callback(recorder)

    def set_market_task(self, task: Any) -> None:
        self._require_shared_state().set_market_task(task)
        self._publish_market_task_callback(task)

    def set_verdict_cache(self, values: list[dict[str, Any]]) -> None:
        self.verdict_cache = values
        self._publish_verdict_cache_callback(values)

    def advance_paper_session_generation(self) -> int:
        generation = self._require_shared_state().advance_paper_session_generation()
        self.paper_session_generation = generation
        self._publish_generation_callback(generation)
        return generation

    def create(self) -> "ApplicationRuntimeBootstrap":
        """Preserve the legacy creation sequence and every shared reference."""
        if self._creation_started:
            raise RuntimeError("ApplicationRuntimeBootstrap.create() may only run once.")
        self._creation_started = True

        core = ApplicationCoreRuntime(
            settings=self.settings,
            finam_token=self.finam_token,
            deposit=self.deposit,
            lot_sizes=self.lot_sizes,
            logger=self.logger,
            set_balance_snapshot=self._set_balance_snapshot,
        ).create()
        self.application_core_runtime = core

        shared = ApplicationSharedStateRuntime(
            logger=self.logger,
            execution_engine=core.execution_engine,
        ).create()
        self.application_shared_state_runtime = shared

        paper_session = PaperSessionCompositionRuntime(
            db_manager=core.db_manager,
            execution_engine=core.execution_engine,
            shadow_tracker=shared.shadow_tracker,
            bbo_replay_recorder=shared.bbo_replay_recorder,
            bbo_replay_recorder_factory=BBOReplayRecorder,
            paper_performance_tracker=paper_performance_tracker,
            paper_shadow_labels=shared.paper_shadow_labels,
            logger=self.logger,
            closed_trades_storage_key=PAPER_CLOSED_TRADES_STORAGE_KEY,
            closed_trades_storage_limit=PAPER_CLOSED_TRADES_STORAGE_LIMIT,
            shadow_outcomes_storage_limit=SHADOW_OUTCOMES_STORAGE_LIMIT,
            set_bbo_replay_recorder=self.set_bbo_replay_recorder,
        ).create()
        self.paper_session_composition_runtime = paper_session

        # Preserve the configured universe and register it before Market starts.
        core.screener.set_universe(self.tickers)

        market = MarketRuntimeCompositionRuntime(
            dependencies={
                "broker": core.broker,
                "screener": core.screener,
                "execution_engine": core.execution_engine,
                "db_manager": core.db_manager,
                "anomaly_queue": shared.anomaly_queue,
                "queue_item_factory": shared.priority_queue_item_factory,
                "shadow_tracker": shared.shadow_tracker,
                "logger": self.logger,
                "terminal_status": self.terminal_status,
                "recorder_provider": lambda: shared.bbo_replay_recorder,
                "session_generation_provider": lambda: shared.paper_session_generation,
                "environment": self.environment,
                "client_id": self.client_id,
                "ai_engine": core.ai_engine,
                "risk_manager": core.risk_manager,
                "session_lock": shared.paper_session_lock,
                "settings": self.settings,
                "background_operations": shared.background_operations,
                "manager": shared.connection_manager,
                "get_open_positions": core.dashboard_application_runtime.get_dashboard_open_positions,
                "refresh_balance_snapshot": core.dashboard_application_runtime.refresh_balance_snapshot,
                "refresh_dashboard_balance_snapshot": core.dashboard_application_runtime.refresh_balance_snapshot,
                "verdict_cache": self.verdict_cache,
                "max_cache_size": self.max_cache_size,
                "balance_snapshot": self.dashboard_balance_snapshot,
                "tickers": self.tickers,
                "finam_trade_lot_sizes": self.finam_trade_lot_sizes,
            },
            paper_session_composition_runtime=paper_session,
            monotonic=self.monotonic,
        ).create()
        self.market_runtime_composition = market

        self.application_composition_root_runtime = ApplicationCompositionRootRuntime(
            application_core_runtime=core,
            application_shared_state_runtime=shared,
            paper_session_composition_runtime=paper_session,
            market_runtime_composition=market,
            settings=self.settings,
            deposit=self.deposit,
            logger=self.logger,
            verdict_cache=self.verdict_cache,
            max_cache_size=self.max_cache_size,
            set_verdict_cache=self.set_verdict_cache,
            set_market_task=self.set_market_task,
            advance_session_generation=self.advance_paper_session_generation,
        ).create()
        self._created = True
        return self
