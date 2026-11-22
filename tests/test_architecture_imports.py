"""Guard the package migration against accidental legacy imports.

Root-level compatibility modules intentionally remain available for external
scripts and older tests. Application packages must import their canonical
package modules directly.
"""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _application_process_runtime_source() -> str:
    return (
        ROOT / "runtime" / "application_process_runtime.py"
    ).read_text(encoding="utf-8")


def _compatibility_exports_source() -> str:
    return (
        ROOT / "runtime" / "application_compatibility_exports.py"
    ).read_text(encoding="utf-8")
PACKAGE_DIRS = (
    "ai",
    "exchange",
    "execution",
    "interface",
    "monitoring",
    "portfolio",
    "risk",
    "runtime",
    "storage",
    "strategy",
    "tools",
)

LEGACY_ROOT_MODULES = {
    "ai_engine",
    "candidate_recovery",
    "dashboard_verdicts",
    "db_manager",
    "evolution_agent",
    "execution_layer",
    "finam_client",
    "knowledge_parser",
    "minute_bar_patterns",
    "news_parser",
    "pnl_exporter",
    "portfolio_metrics",
    "rag_prompt_utils",
    "risk_limits",
    "risk_manager",
    "scalping_strategy",
    "screener_funnel",
    "shadow_outcomes",
    "signal_guard",
    "tg_bot",
}


def _legacy_imports(source: str, filename: str) -> list[str]:
    tree = ast.parse(source, filename=filename)
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root_name = alias.name.split(".", 1)[0]
                if root_name in LEGACY_ROOT_MODULES:
                    found.append(f"{filename}:{node.lineno}: import {alias.name}")
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            root_name = node.module.split(".", 1)[0]
            if root_name in LEGACY_ROOT_MODULES:
                found.append(
                    f"{filename}:{node.lineno}: from {node.module} import ..."
                )
        elif isinstance(node, ast.Call):
            func = node.func
            is_import_module = (
                isinstance(func, ast.Name) and func.id == "__import__"
            ) or (
                isinstance(func, ast.Attribute) and func.attr == "import_module"
            )
            if is_import_module and node.args:
                arg = node.args[0]
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    root_name = arg.value.split(".", 1)[0]
                    if root_name in LEGACY_ROOT_MODULES:
                        found.append(
                            f"{filename}:{node.lineno}: dynamic import {arg.value}"
                        )
    return found


def test_application_packages_do_not_import_legacy_root_shims() -> None:
    violations: list[str] = []
    for package in PACKAGE_DIRS:
        package_root = ROOT / package
        if not package_root.exists():
            continue
        for path in sorted(package_root.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            violations.extend(
                _legacy_imports(path.read_text(encoding="utf-8"), str(path.relative_to(ROOT)))
            )

    assert not violations, (
        "Application packages must use canonical package imports; found legacy "
        "root-shim dependencies:\\n" + "\\n".join(violations)
    )


def test_main_uses_canonical_package_imports() -> None:
    main_path = ROOT / "main.py"
    violations = _legacy_imports(main_path.read_text(encoding="utf-8"), "main.py")
    assert not violations, (
        "main.py is the supported root entry point, but its internal imports "
        "must resolve through packages:\\n" + "\\n".join(violations)
    )

def test_logging_setup_owns_terminal_status_implementations() -> None:
    source = (ROOT / "monitoring/logging_setup.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="monitoring/logging_setup.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.terminal_status"
        for alias in node.names
    }
    assert {"TerminalStatusLine", "StatusAwareStreamHandler"} <= imported

    main_source = (ROOT / "main.py").read_text(encoding="utf-8")
    main_tree = ast.parse(main_source, filename="main.py")
    local_classes = {
        node.name for node in main_tree.body if isinstance(node, ast.ClassDef)
    }
    assert "TerminalStatusLine" not in local_classes
    assert "StatusAwareStreamHandler" not in local_classes

def test_market_runtime_composition_imports_reason_classifier_from_monitoring() -> None:
    composition_source = (
        ROOT / "runtime" / "market_runtime_composition_runtime.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(
        composition_source,
        filename="runtime/market_runtime_composition_runtime.py",
    )
    imports = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.trade_flow_diagnostics"
        for alias in node.names
    }
    assert imports == {"guard_reason_bucket"}


def test_guard_reason_bucket_preserves_diagnostic_categories() -> None:
    from monitoring.trade_flow_diagnostics import guard_reason_bucket

    cases = {
        "Bid/Ask котировки неактуальны": "quotes",
        "Спред выше лимита": "spread",
        "ATR M1-история недостаточна": "atr",
        "Сокращение размера лучшей заявки": "size",
        "Всплеск объёма": "volume",
        "Импульс против VWAP": "direction",
        "неизвестная причина": "other",
    }
    assert {reason: guard_reason_bucket(reason) for reason in cases} == cases


def test_bbo_resume_reason_bucket_preserves_diagnostic_categories() -> None:
    from monitoring.trade_flow_diagnostics import bbo_resume_reason_bucket

    cases = {
        "состояние котировок отсутствует": "quote_missing",
        "стакан не подтверждён": "book_unconfirmed",
        "непротиворечивый двусторонний BBO отсутствует": "invalid_bbo",
        "ORDER_BOOK устарело": "book_stale",
        "нет свежего подтверждения": "confirmation_stale",
        "другая причина": "other_book_failure",
    }
    assert {reason: bbo_resume_reason_bucket(reason) for reason in cases} == cases


def test_main_delegates_trade_flow_state_to_market_runtime_composition() -> None:
    main_source = (ROOT / "main.py").read_text(encoding="utf-8")
    composition_source = (
        ROOT / "runtime" / "market_runtime_composition_runtime.py"
    ).read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "monitoring" / "market_observability_runtime.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(
        composition_source,
        filename="runtime/market_runtime_composition_runtime.py",
    )
    imports = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.market_observability_runtime"
        for alias in node.names
    }
    assert imports == {"MarketObservabilityRuntime"}
    assert '"market_observability_runtime": observability' in _compatibility_exports_source()
    assert '"_trade_flow_counters": observability.trade_flow_counters' in _compatibility_exports_source()
    assert "MarketObservabilityRuntime(" in composition_source
    assert "self.trade_flow_counters = TradeFlowCounters(monotonic())" in runtime_source
    assert "self.trade_flow_counters.increment(key, amount)" in runtime_source
    assert "_trade_flow_lock" not in main_source
    assert "_trade_flow_counts" not in main_source
    assert "_trade_flow_last_log_at" not in main_source


def test_application_shared_state_runtime_owns_websocket_manager() -> None:
    main_source = (ROOT / "main.py").read_text(encoding="utf-8")
    shared_source = (
        ROOT / "runtime" / "application_shared_state_runtime.py"
    ).read_text(encoding="utf-8")
    assert '"manager": shared.connection_manager' in _compatibility_exports_source()
    assert "from interface.websocket_manager import ConnectionManager" in shared_source
    assert "connection_manager = self.connection_manager_factory()" in shared_source

def test_main_delegates_runtime_logging_to_monitoring() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    local_classes = {
        node.name for node in tree.body if isinstance(node, ast.ClassDef)
    }
    assert not {
        "_PrivacyFormatter",
        "_DurableFileHandler",
        "_RunIdFilter",
        "_DiagnosticLogFilter",
        "_SessionLogFilter",
        "_ErrorLogFilter",
    } & local_classes

    process_tree = ast.parse(
        _application_process_runtime_source(),
        filename="runtime/application_process_runtime.py",
    )
    imported = {
        alias.name
        for node in process_tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.logging_setup"
        for alias in node.names
    }
    assert imported == {"logger", "terminal_status"}
    market_composition_source = (
        ROOT / "runtime" / "market_runtime_composition_runtime.py"
    ).read_text(encoding="utf-8")
    market_composition_tree = ast.parse(
        market_composition_source,
        filename="runtime/market_runtime_composition_runtime.py",
    )
    market_logging_imports = {
        alias.name
        for node in market_composition_tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.logging_setup"
        for alias in node.names
    }
    assert market_logging_imports == {"update_numpy_impulse_status"}

    event_logger_source = (ROOT / "monitoring/trading_event_logger.py").read_text(encoding="utf-8")
    event_logger_tree = ast.parse(event_logger_source, filename="monitoring/trading_event_logger.py")
    event_logger_imports = {
        alias.name
        for node in event_logger_tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.logging_setup"
        for alias in node.names
    }
    assert {"RUN_ID", "logger"} <= event_logger_imports



def test_application_shared_state_runtime_owns_background_operation_tracking() -> None:
    main_source = (ROOT / "main.py").read_text(encoding="utf-8")
    shared_source = (
        ROOT / "runtime" / "application_shared_state_runtime.py"
    ).read_text(encoding="utf-8")
    assert '"_background_operations": shared.background_operations' in _compatibility_exports_source()
    assert "from monitoring.background_tasks import BackgroundOperationTracker" in shared_source
    assert "background_operations = self.background_operations_factory(self.logger)" in shared_source
    assert "_tracked_operation_tasks" not in main_source
    assert "def _spawn_tracked_operation" not in main_source
    assert "async def _drain_tracked_operations" not in main_source



def test_main_delegates_trading_event_journal_to_monitoring() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    composition_source = (
        ROOT / "runtime" / "market_runtime_composition_runtime.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(
        composition_source,
        filename="runtime/market_runtime_composition_runtime.py",
    )
    imports = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.trading_event_logger"
        for alias in node.names
    }
    assert imports == {"log_trading_event_to_file"}
    assert '"log_trading_event": log_trading_event_to_file' in composition_source
    assert "async def log_trading_event_to_file" not in source
    assert "_sync_write_trading_log" not in source



def test_main_delegates_position_order_book_diagnostics_to_monitoring() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "monitoring" / "market_observability_runtime.py"
    ).read_text(encoding="utf-8")
    clear_source = (
        ROOT / "storage" / "paper_session_clear.py"
    ).read_text(encoding="utf-8")
    assert '"position_order_book_diagnostics": observability.position_order_book_diagnostics' in _compatibility_exports_source()
    assert "PositionOrderBookDiagnostics(" in runtime_source
    assert "self.position_order_book_diagnostics.record(" in runtime_source
    assert "position_order_book_diagnostics.clear()" in clear_source
    assert "_position_order_book_diag_lock" not in source
    assert "_position_order_book_diag:" not in source


def test_main_delegates_prefilter_warning_rate_limiting_to_monitoring() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "monitoring" / "market_observability_runtime.py"
    ).read_text(encoding="utf-8")
    assert '"prefilter_block_logger": observability.prefilter_block_logger' in _compatibility_exports_source()
    assert "PrefilterBlockLogger(" in runtime_source
    assert "self.prefilter_block_logger.log(" in runtime_source
    assert "_prefilter_last_logged" not in source
    assert "_prefilter_log_lock" not in source


def test_main_delegates_strategy_shadow_scorecard_logging_to_monitoring() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "monitoring" / "market_observability_runtime.py"
    ).read_text(encoding="utf-8")
    assert '"shadow_scorecard_logger": observability.shadow_scorecard_logger' in _compatibility_exports_source()
    assert "StrategyShadowScorecardLogger(" in runtime_source
    assert "self.shadow_scorecard_logger.log(self.shadow_tracker)" in runtime_source
    assert "scalping_shadow_tracker.statistics_snapshot()" not in source


def test_main_delegates_trade_flow_report_formatting_to_monitoring() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "monitoring" / "market_observability_runtime.py"
    ).read_text(encoding="utf-8")
    assert '"trade_flow_report_logger": observability.trade_flow_report_logger' in _compatibility_exports_source()
    assert "TradeFlowReportLogger(" in runtime_source
    assert "self.trade_flow_report_logger.log(counts, pending_bbo=pending_bbo)" in runtime_source


def test_main_delegates_strategy_shadow_events_to_monitoring() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "monitoring" / "market_observability_runtime.py"
    ).read_text(encoding="utf-8")
    assert '"strategy_shadow_diagnostics": observability.strategy_shadow_diagnostics' in _compatibility_exports_source()
    assert "StrategyShadowDiagnostics(" in runtime_source
    assert "self.strategy_shadow_diagnostics.mark_llm_not_called(" in runtime_source
    assert "self.strategy_shadow_diagnostics.log_candidate(" in runtime_source
    assert '"_mark_shadow_llm_not_called": observability.mark_shadow_llm_not_called' in _compatibility_exports_source()
    assert '"_log_scalping_shadow": observability.log_shadow_candidate' in _compatibility_exports_source()


def test_main_delegates_priority_queue_state_to_application_shared_runtime() -> None:
    main_source = (ROOT / "main.py").read_text(encoding="utf-8")
    shared_source = (
        ROOT / "runtime" / "application_shared_state_runtime.py"
    ).read_text(encoding="utf-8")
    assert '"priority_queue_item_factory": shared.priority_queue_item_factory' in _compatibility_exports_source()
    assert "from runtime.priority_queue import PriorityItem, PriorityQueueItemFactory" in shared_source
    assert "asyncio.PriorityQueue[" in shared_source
    assert "maxsize=1000" in shared_source
    assert "PriorityQueueItemFactory()" in shared_source
    assert "def _make_priority_item" not in main_source
    assert "_priority_sequence" not in main_source
    assert "from itertools import count" not in main_source



def test_main_delegates_paper_open_pnl_accounting_to_portfolio() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    bootstrap_source = (
        ROOT / "runtime" / "application_runtime_bootstrap.py"
    ).read_text(encoding="utf-8")
    assert "from runtime.application_core_runtime import ApplicationCoreRuntime" in bootstrap_source
    core_source = (
        ROOT / "runtime" / "application_core_runtime.py"
    ).read_text(encoding="utf-8")
    core_tree = ast.parse(
        core_source, filename="runtime/application_core_runtime.py"
    )
    dashboard_imports = {
        alias.name
        for node in core_tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "runtime.dashboard_application_runtime"
        for alias in node.names
    }
    assert dashboard_imports == {"DashboardApplicationRuntime"}
    assert "DashboardApplicationRuntime(" in core_source
    composition_source = (
        ROOT / "runtime" / "market_runtime_composition_runtime.py"
    ).read_text(encoding="utf-8")
    composition_tree = ast.parse(
        composition_source,
        filename="runtime/market_runtime_composition_runtime.py",
    )
    accounting_imports = {
        alias.name
        for node in composition_tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "portfolio.paper_accounting"
        for alias in node.names
    }
    assert accounting_imports == {"parse_finite_float"}
    assert "self.parse_finite_float = parse_finite_float" in composition_source
    assert '"_current_paper_open_pnl": dashboard.current_paper_open_pnl' in _compatibility_exports_source()
    assert '"_current_paper_assets_cost": dashboard.current_paper_assets_cost' in _compatibility_exports_source()

def test_main_delegates_dashboard_balance_runtime_to_portfolio() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    composition_source = (
        ROOT / "runtime" / "dashboard_application_runtime.py"
    ).read_text(encoding="utf-8")
    core_source = (
        ROOT / "runtime" / "application_core_runtime.py"
    ).read_text(encoding="utf-8")
    assert "DashboardApplicationRuntime(" in core_source
    assert '"dashboard_application_runtime": dashboard' in _compatibility_exports_source()
    assert '"_refresh_dashboard_balance_snapshot": dashboard.refresh_balance_snapshot' in _compatibility_exports_source()
    assert "self.dashboard_runtime.refresh_balance_snapshot(" in composition_source
    assert "self.dashboard_runtime.reset_balance_guard_signature()" in composition_source
    runtime_source = (
        ROOT / "portfolio" / "dashboard_runtime.py"
    ).read_text(encoding="utf-8")
    assert "build_dashboard_balance_snapshot(" in runtime_source
    assert "reset_balance_guard_signature" in runtime_source


def test_main_delegates_core_service_and_capital_composition_to_runtime() -> None:
    main_source = (ROOT / "main.py").read_text(encoding="utf-8")
    core_source = (
        ROOT / "runtime" / "application_core_runtime.py"
    ).read_text(encoding="utf-8")
    bootstrap_source = (
        ROOT / "runtime" / "application_runtime_bootstrap.py"
    ).read_text(encoding="utf-8")
    assert "bootstrap_factory: Callable[..., Any] = ApplicationRuntimeBootstrap" in _application_process_runtime_source()
    assert "self.bootstrap_factory(" in _application_process_runtime_source()
    assert "ApplicationCoreRuntime(" in bootstrap_source
    for direct_constructor in (
        "DBManager()",
        "AIEngine()",
        "FinamBrokerClient(",
        "HFTMemoryScreener(",
        "BaseExecutionEngine(",
        "RiskManager(",
        "DashboardApplicationRuntime(",
        "math.isclose(",
    ):
        assert direct_constructor not in main_source
    for delegated_constructor in (
        "DBManager()",
        "AIEngine()",
        "FinamBrokerClient(",
        "HFTMemoryScreener(window_size=30)",
        "BaseExecutionEngine(",
        "RiskManager(",
        "DashboardApplicationRuntime(",
    ):
        assert delegated_constructor in core_source
    assert "self.broker.open_positions_provider" in core_source
    assert "float(self.execution_engine.initial_capital)" in core_source
    assert "float(self.risk_manager.initial_deposit)" in core_source
    assert 'abs_tol=0.01' in core_source
    assert "self.risk_manager.open_pnl_provider" in core_source
    assert "self.risk_manager.open_assets_cost_provider" in core_source
    assert '"dashboard_application_runtime": dashboard' in _compatibility_exports_source()

def test_main_delegates_shadow_outcome_persistence_to_storage() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    composition_source = (
        ROOT / "runtime" / "paper_session_composition_runtime.py"
    ).read_text(encoding="utf-8")
    bootstrap_source = (
        ROOT / "runtime" / "application_runtime_bootstrap.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "storage.shadow_outcomes"
        for alias in node.names
    }
    assert "from storage.shadow_outcomes import SHADOW_OUTCOMES_STORAGE_LIMIT" in bootstrap_source
    assert "ApplicationProcessRuntime(" in source

    composition_imports = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "runtime.paper_session_composition_runtime"
        for alias in node.names
    }
    assert "from runtime.paper_session_composition_runtime import PaperSessionCompositionRuntime" in bootstrap_source
    assert "PaperSessionCompositionRuntime(" in bootstrap_source
    assert '"shadow_outcome_persistence": paper.shadow_outcome_persistence' in _compatibility_exports_source()
    assert '"paper_session_runtime": paper.paper_session_runtime' in _compatibility_exports_source()
    assert '"_persist_shadow_outcomes": paper.persist_shadow_outcomes' in _compatibility_exports_source()
    assert '"_restore_shadow_outcomes": paper.restore_shadow_outcomes' in _compatibility_exports_source()
    assert '"_rotate_paper_bbo_replay_session": paper.rotate_bbo_replay_session' in _compatibility_exports_source()
    assert '"_restore_paper_performance": paper.restore_paper_performance' in _compatibility_exports_source()

    assert "ShadowOutcomePersistence(" in composition_source
    assert "PaperSessionRuntime(" in composition_source
    assert "await self.paper_session_runtime.persist_shadow_outcomes(" in composition_source
    assert "await self.paper_session_runtime.restore_shadow_outcomes()" in composition_source
    assert "await self.paper_session_runtime.rotate_bbo_replay_session()" in composition_source
    assert "self.set_bbo_replay_recorder(recorder)" in composition_source
    assert "await self.paper_session_runtime.restore_paper_performance()" in composition_source
    assert "persist_completed_labels(" not in source
    assert "restore_completed_labels(" not in source


def test_main_delegates_market_data_runtime_services() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    application_source = (
        ROOT / "runtime" / "market_application_runtime.py"
    ).read_text(encoding="utf-8")
    application_tree = ast.parse(
        application_source,
        filename="runtime/market_application_runtime.py",
    )
    imported = {
        alias.name
        for node in application_tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "strategy.market_data_runtime"
        for alias in node.names
    }
    assert imported == {"paper_queue_flow_conflict_reason"}
    factory_source = (
        ROOT / "runtime" / "market_ingestion_runtime.py"
    ).read_text(encoding="utf-8")
    assert "class MarketIngestionRuntimeFactory" in factory_source
    assert "M1HistoryWarmup(" in factory_source
    assert "PendingBBOCandidateStore(" in factory_source
    assert '"m1_history_warmup": ingestion.m1_history_warmup' in _compatibility_exports_source()
    assert '"pending_bbo_candidate_store": ingestion.pending_bbo_candidate_store' in _compatibility_exports_source()
    assert "coalesce_pending_bbo_entry(" not in source
    assert "bars = await broker.get_historical_m1_bars(" not in source


def test_main_delegates_paper_api_helpers_to_interface() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "runtime" / "paper_operations_runtime.py"
    ).read_text(encoding="utf-8")
    assert '"paper_operations_runtime": paper_ops' in _compatibility_exports_source()
    assert "PaperOperationsRuntime(" in (ROOT / "runtime" / "application_composition_runtime.py").read_text(encoding="utf-8")
    assert "PaperAPIHelpers(" in runtime_source
    assert "self.paper_api_helpers.require_paper_mode()" in runtime_source
    assert "self.paper_api_helpers.paper_trade_rows(snapshot)" in runtime_source
    assert "self.paper_api_helpers.broadcast_paper_tables()" in runtime_source
    assert '"paper_api_helpers": paper_ops.paper_api_helpers' in _compatibility_exports_source()
    assert '"_broadcast_paper_tables": paper_ops.broadcast_paper_tables' in _compatibility_exports_source()


def test_main_delegates_paper_research_and_reconciliation_apis_to_storage() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    composition_source = (
        ROOT / "runtime" / "paper_operations_runtime.py"
    ).read_text(encoding="utf-8")
    router_source = (
        ROOT / "interface" / "paper_research_routes.py"
    ).read_text(encoding="utf-8")
    main_tree = ast.parse(source, filename="main.py")
    router_tree = ast.parse(router_source, filename="paper_research_routes.py")

    assert "PaperResearchRuntime(" in composition_source
    assert '"paper_research_runtime": paper_ops.paper_research_runtime' in _compatibility_exports_source()
    assert "create_paper_research_router(self.paper_research_runtime)" in composition_source

    route_methods = {
        "get_paper_performance": ("get", "/api/v1/paper/performance", "get_performance"),
        "replay_paper_exit_batch": ("get", "/api/v1/paper/replay/batch", "replay_batch"),
        "get_paper_shadow_reconciliation": (
            "get", "/api/v1/paper/reconciliation/shadow", "get_shadow_reconciliation"
        ),
        "get_paper_shadow_walk_forward": (
            "get", "/api/v1/paper/shadow/walk-forward", "get_shadow_walk_forward"
        ),
        "get_paper_shadow_cost_sensitivity": (
            "get", "/api/v1/paper/shadow/cost-sensitivity", "get_shadow_cost_sensitivity"
        ),
        "get_paper_research_report": (
            "get", "/api/v1/paper/research/report", "get_research_report"
        ),
        "replay_paper_exit_scenarios": (
            "get", "/api/v1/paper/replay/{order_id}", "replay_trade"
        ),
        "get_paper_reconciliation": (
            "get", "/api/v1/paper/reconciliation", "get_reconciliation"
        ),
        "reconcile_paper_position": (
            "post", "/api/v1/paper/reconciliation/{order_id}", "reconcile_position"
        ),
    }
    functions = {
        node.name: node
        for node in ast.walk(router_tree)
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
    }
    assert set(route_methods) <= set(functions)

    observed_routes = set()
    for function_name, (http_method, route_path, runtime_method) in route_methods.items():
        function = functions[function_name]
        decorators = [
            decorator
            for decorator in function.decorator_list
            if isinstance(decorator, ast.Call)
            and isinstance(decorator.func, ast.Attribute)
            and decorator.func.attr.lower() == http_method
        ]
        assert len(decorators) == 1, function_name
        assert decorators[0].args and isinstance(decorators[0].args[0], ast.Constant)
        assert decorators[0].args[0].value == route_path, function_name
        calls = [
            node for node in ast.walk(function)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == runtime_method
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "runtime"
        ]
        assert len(calls) == 1, function_name
        observed_routes.add((http_method.upper(), route_path))

    assert len(observed_routes) == 9
    legacy_handlers = {
        node.name for node in main_tree.body
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
    }
    assert not set(route_methods) & legacy_handlers


def test_application_composition_root_uses_canonical_shadow_retention_limit() -> None:
    root_source = (
        ROOT / "runtime" / "application_composition_root_runtime.py"
    ).read_text(encoding="utf-8")
    composition_source = (
        ROOT / "runtime" / "paper_operations_runtime.py"
    ).read_text(encoding="utf-8")
    assert "from storage.shadow_outcomes import SHADOW_OUTCOMES_STORAGE_LIMIT" in root_source
    assert '"shadow_outcomes_storage_limit": SHADOW_OUTCOMES_STORAGE_LIMIT' in root_source
    assert 'shadow_outcomes_storage_limit=deps["shadow_outcomes_storage_limit"]' in composition_source



def test_main_delegates_fastapi_application_assembly_to_composition_runtimes() -> None:
    main_source = (ROOT / "main.py").read_text(encoding="utf-8")
    root_source = (
        ROOT / "runtime" / "application_composition_root_runtime.py"
    ).read_text(encoding="utf-8")
    composition_source = (
        ROOT / "runtime" / "application_composition_runtime.py"
    ).read_text(encoding="utf-8")
    bootstrap_source = (
        ROOT / "runtime" / "application_runtime_bootstrap.py"
    ).read_text(encoding="utf-8")
    assert "bootstrap_factory: Callable[..., Any] = ApplicationRuntimeBootstrap" in _application_process_runtime_source()
    assert "self.bootstrap_factory(" in _application_process_runtime_source()
    assert "ApplicationCompositionRootRuntime(" in bootstrap_source
    assert "ApplicationCompositionRuntime(" not in main_source
    assert "ApplicationLifecycleRuntime(" not in main_source
    assert "FastAPI(" not in main_source
    assert "StaticFiles(" not in main_source
    assert "PaperOperationsRuntime(" not in main_source
    assert "ApplicationCompositionRuntime(" in root_source
    assert "ApplicationLifecycleRuntime(" in composition_source
    assert "FastAPI(title=self.app_title, lifespan=lifecycle_runtime.lifespan)" in composition_source
    assert "dashboard_application_runtime.register_routes(" in composition_source
    assert ").create(app)" in composition_source
    assert '"/static"' in composition_source
    assert '"application_lifecycle_runtime": composition.application_lifecycle_runtime' in _compatibility_exports_source()
    assert '"paper_operations_runtime": paper_ops' in _compatibility_exports_source()

def test_lifespan_delegates_long_running_task_ownership_to_supervisor() -> None:
    main_source = (ROOT / "main.py").read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "runtime" / "application_lifecycle_runtime.py"
    ).read_text(encoding="utf-8")
    main_tree = ast.parse(main_source, filename="main.py")
    runtime_tree = ast.parse(
        runtime_source, filename="runtime/application_lifecycle_runtime.py"
    )

    root_source = (
        ROOT / "runtime" / "application_composition_root_runtime.py"
    ).read_text(encoding="utf-8")
    root_tree = ast.parse(
        root_source,
        filename="runtime/application_composition_root_runtime.py",
    )
    imports = {
        alias.name
        for node in root_tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.background_tasks"
        for alias in node.names
    }
    assert imports == {"BackgroundTaskSupervisor"}
    shared_source = (
        ROOT / "runtime" / "application_shared_state_runtime.py"
    ).read_text(encoding="utf-8")
    assert "BackgroundOperationTracker" in shared_source

    lifecycle_class = next(
        node for node in runtime_tree.body
        if isinstance(node, ast.ClassDef)
        and node.name == "ApplicationLifecycleRuntime"
    )
    lifespan = next(
        node for node in lifecycle_class.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "lifespan"
    )
    lifespan_source = ast.get_source_segment(runtime_source, lifespan) or ""
    assert "self.task_supervisor_factory(logger)" in lifespan_source
    assert "background_task_supervisor.spawn(" in lifespan_source
    assert "background_task_supervisor.cancel_and_drain()" in lifespan_source
    assert "_spawn_background" not in lifespan_source
    assert "background_tasks: set[asyncio.Task]" not in lifespan_source


def test_main_delegates_market_event_normalization_to_runtime() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    composition_source = (
        ROOT / "runtime" / "market_runtime_composition_runtime.py"
    ).read_text(encoding="utf-8")
    application_source = (
        ROOT / "runtime" / "market_application_runtime.py"
    ).read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "runtime" / "market_data_event_runtime.py"
    ).read_text(encoding="utf-8")
    normalizer_source = (
        ROOT / "runtime" / "market_event_normalizer.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    bootstrap_source = (
        ROOT / "runtime" / "application_runtime_bootstrap.py"
    ).read_text(encoding="utf-8")
    assert "from runtime.market_runtime_composition_runtime import MarketRuntimeCompositionRuntime" in bootstrap_source
    assert "MarketRuntimeCompositionRuntime(" in bootstrap_source
    assert "self.market_application_runtime = MarketApplicationRuntime(" in composition_source
    assert '"market_application_runtime": market_app' in _compatibility_exports_source()
    assert '"market_ingestion_components": ingestion' in _compatibility_exports_source()
    assert "from runtime.market_ingestion_runtime import MarketIngestionRuntimeFactory" in application_source
    assert "create_market_ingestion_components(" in application_source
    assert "self, MarketIngestionRuntimeFactory" in application_source
    assert "create_market_worker_factory(" in application_source
    assert "self, MarketWorkerFactory" in application_source
    worker_composition_source = (
        ROOT / "runtime" / "market_application_worker_composition.py"
    ).read_text(encoding="utf-8")
    assert "factory_cls(" in worker_composition_source
    ingestion_composition_source = (
        ROOT / "runtime" / "market_application_ingestion_composition.py"
    ).read_text(encoding="utf-8")
    assert "return factory_cls(" in ingestion_composition_source
    assert "create_market_background_services(" in application_source
    assert "self, MarketBackgroundServicesRuntime" in application_source
    assert '"queue_flow_conflict_reason": runtime.paper_queue_flow_conflict_reason' in ingestion_composition_source
    assert '"market_data_event_runtime": ingestion.market_data_event_runtime' in _compatibility_exports_source()
    assert '"process_market_event": market_app.process_market_event' in _compatibility_exports_source()
    assert "async def process_market_event(self, event: Any)" in application_source
    assert "await self._require_ingestion().market_data_event_runtime.process_event(event)" in application_source
    assert "quote_event = normalize_market_quote_event(event)" in runtime_source
    assert "def normalize_market_quote_event(" in normalizer_source
    assert "self.candidate_pipeline.process_candidate(" in runtime_source
    assert "build_market_candidate_payload(" not in runtime_source


def test_main_delegates_auxiliary_market_events_and_bbo_replay_to_runtime() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "runtime" / "market_data_event_runtime.py"
    ).read_text(encoding="utf-8")
    router_source = (
        ROOT / "runtime" / "market_event_router.py"
    ).read_text(encoding="utf-8")
    factory_source = (
        ROOT / "runtime" / "market_ingestion_runtime.py"
    ).read_text(encoding="utf-8")
    assert "MarketAuxiliaryEventRouter(" in factory_source
    assert "PaperBBOReplayAdapter(" in factory_source
    assert "if await self.auxiliary_event_router.process_if_handled(event):" in runtime_source
    assert "self.auxiliary_event_router.bbo_replay.record(" in runtime_source
    assert "await self.screener.push_order_book(**normalized)" in router_source
    assert "await self.pending_bbo_runtime.resume(ticker, trigger=\"QUOTES\")" in runtime_source


def test_main_delegates_candidate_admission_to_strategy_pipeline() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    composition_source = (
        ROOT / "runtime" / "market_application_runtime.py"
    ).read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "runtime" / "market_data_event_runtime.py"
    ).read_text(encoding="utf-8")
    pipeline_source = (
        ROOT / "strategy" / "market_candidate_pipeline.py"
    ).read_text(encoding="utf-8")
    factory_source = (
        ROOT / "runtime" / "market_ingestion_runtime.py"
    ).read_text(encoding="utf-8")
    assert "MarketCandidatePipeline(" in factory_source
    assert "MarketRuntimeCompositionRuntime(" in (
        ROOT / "runtime" / "application_runtime_bootstrap.py"
    ).read_text(encoding="utf-8")
    assert "MarketApplicationRuntime(" in (
        ROOT / "runtime" / "market_runtime_composition_runtime.py"
    ).read_text(encoding="utf-8")
    assert '"market_candidate_pipeline": ingestion.market_candidate_pipeline' in _compatibility_exports_source()
    assert "create_market_ingestion_components(" in composition_source
    assert "self, MarketIngestionRuntimeFactory" in composition_source
    ingestion_composition_source = (
        ROOT / "runtime" / "market_application_ingestion_composition.py"
    ).read_text(encoding="utf-8")
    assert "return factory_cls(" in ingestion_composition_source
    assert "self.candidate_pipeline.process_candidate(" in runtime_source
    assert "validate_atr_readiness" not in runtime_source
    assert "infer_candidate_direction" not in runtime_source
    assert "self.infer_direction(metrics)" in pipeline_source
    assert "validate_entry_evidence" in pipeline_source
    assert "validate_atr_readiness" in pipeline_source
    assert "anomaly_queue.put_nowait" in pipeline_source


def test_main_delegates_pending_bbo_candidate_lifecycle_to_strategy_runtime() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    composition_source = (
        ROOT / "runtime" / "market_application_runtime.py"
    ).read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "strategy" / "market_data_runtime.py"
    ).read_text(encoding="utf-8")
    factory_source = (
        ROOT / "runtime" / "market_ingestion_runtime.py"
    ).read_text(encoding="utf-8")
    assert "PendingBBOCandidateRuntime(" in factory_source
    assert '"pending_bbo_runtime": ingestion.pending_bbo_runtime' in _compatibility_exports_source()
    event_runtime_source = (
        ROOT / "runtime" / "market_data_event_runtime.py"
    ).read_text(encoding="utf-8")
    assert "self.pending_bbo_runtime.has_pending(ticker)" in event_runtime_source
    assert 'await self.pending_bbo_runtime.resume(ticker, trigger="QUOTES")' in event_runtime_source
    assert "def store_pending_bbo_candidate(" in composition_source
    assert "pending_bbo_candidate_store.store_candidate(" in composition_source
    assert '"_resume_pending_bbo_candidate": market_app.resume_pending_bbo_candidate' in _compatibility_exports_source()

    runtime_tree = ast.parse(
        runtime_source, filename="strategy/market_data_runtime.py"
    )
    runtime_class = next(
        node for node in runtime_tree.body
        if isinstance(node, ast.ClassDef)
        and node.name == "PendingBBOCandidateRuntime"
    )
    methods = {
        node.name for node in runtime_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert {
        "drop", "cancel", "expire", "store_candidate", "has_pending", "resume"
    } <= methods
    for guard in (
        "PAPER_SESSION_CHANGED",
        "FUNNEL_NOT_READY",
        "TOP15_DESELECTED",
        "POSITION_ALREADY_OPEN",
        "DIRECTION_CHANGED",
        "QUEUE_TRADE_FLOW_CONFLICT",
        "SIGNAL_EVIDENCE_BLOCKED",
        "ATR_NOT_READY",
        "PAYLOAD_SUPERSEDED",
        "AI_DEDUPLICATED",
        "AI_QUEUE_FULL",
    ):
        assert guard in runtime_source


def test_market_worker_delegates_order_execution_lifecycle_to_execution_package() -> None:
    composition_source = (
        ROOT / "runtime" / "market_application_runtime.py"
    ).read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "execution" / "market_worker_execution_runtime.py"
    ).read_text(encoding="utf-8")
    worker_source = (ROOT / "runtime" / "market_worker.py").read_text(encoding="utf-8")
    worker_tree = ast.parse(worker_source, filename="runtime/market_worker.py")
    worker_imports = {
        alias.name
        for node in worker_tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "execution.market_worker_execution_runtime"
        for alias in node.names
    }
    assert worker_imports == {"MarketWorkerExecutionRuntime"}
    assert "create_market_worker_factory(" in composition_source
    assert "self, MarketWorkerFactory" in composition_source
    worker_composition_source = (
        ROOT / "runtime" / "market_application_worker_composition.py"
    ).read_text(encoding="utf-8")
    assert "factory_cls(" in worker_composition_source
    assert "self.execution_runtime = MarketWorkerExecutionRuntime(**execution_dependencies)" in worker_source
    assert '"normalize_execution_fill": callbacks["normalize_execution_fill"]' in worker_composition_source
    assert '"publish_execution_outcome": callbacks["publish_execution_outcome"]' in worker_composition_source

    worker = next(
        node for node in worker_tree.body
        if isinstance(node, ast.ClassDef) and node.name == "MarketWorker"
    )
    execution_methods = {
        node.name: ast.get_source_segment(worker_source, node) or ""
        for node in worker.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name in {"_execute_and_publish", "_execute_and_publish_locked"}
    }
    assert "self.execution_runtime.execute_and_publish(" in execution_methods["_execute_and_publish"]
    assert "self.execution_runtime.execute_and_publish_locked(" in execution_methods["_execute_and_publish_locked"]
    assert "await execution_engine.execute_order(" not in worker_source
    assert "await publish_execution_outcome(" not in worker_source
    assert "await self.execution_engine.execute_order(" in runtime_source
    assert "force_refresh=True" in runtime_source
    assert "candidate_is_expired(" in runtime_source
    assert "self.normalize_execution_fill(" in runtime_source
    assert "await self.publish_execution_outcome(" in runtime_source


def test_main_delegates_market_verdict_publication_to_execution_package() -> None:
    composition_source = (
        ROOT / "runtime" / "market_runtime_composition_runtime.py"
    ).read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "runtime" / "market_worker_queue_processing_runtime.py"
    ).read_text(encoding="utf-8")
    module_source = (
        ROOT / "execution" / "verdict_publisher.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(
        composition_source,
        filename="runtime/market_runtime_composition_runtime.py",
    )
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "execution.verdict_publisher"
        for alias in node.names
    }
    assert imported == {"publish_market_verdict"}
    assert '"publish_market_verdict": publish_market_verdict' in composition_source
    assert "await publish_market_verdict(" in runtime_source
    assert "manager.broadcast(json.dumps({\"type\": \"EXECUTION_UPDATE\"" not in runtime_source
    assert "verdict_cache.append(verdict)" in module_source


def test_main_delegates_verdict_record_construction_and_enrichment_to_strategy() -> None:
    composition_source = (
        ROOT / "runtime" / "market_runtime_composition_runtime.py"
    ).read_text(encoding="utf-8")
    queue_source = (
        ROOT / "runtime" / "market_worker_queue_processing_runtime.py"
    ).read_text(encoding="utf-8")
    risk_source = (
        ROOT / "runtime" / "market_worker_risk_verdict_runtime.py"
    ).read_text(encoding="utf-8")
    module_source = (
        ROOT / "strategy" / "verdict_records.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(
        composition_source,
        filename="runtime/market_runtime_composition_runtime.py",
    )
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "strategy.verdict_records"
        for alias in node.names
    }
    assert imported == {
        "build_default_signal_reason",
        "build_llm_verdict_record",
        "build_pre_llm_risk_block_verdict",
        "classify_local_evidence_diagnostic",
        "enrich_ai_analysis_with_verdict",
        "link_shadow_candidate_verdict",
    }
    assert '"build_pre_llm_risk_block_verdict": build_pre_llm_risk_block_verdict' in composition_source
    assert '"build_llm_verdict_record": build_llm_verdict_record' in composition_source
    assert "verdict = self.build_pre_llm_risk_block_verdict(" in risk_source
    assert "verdict_data = build_llm_verdict_record(" in queue_source
    assert "link_shadow_candidate_verdict(" in queue_source
    assert "enrich_ai_analysis_with_verdict(" in queue_source
    assert "decision_source" in module_source
    assert "llm_reason_code_raw" in module_source
    assert "market_regime" in module_source
    assert "scalping_shadow_tracker.attach_llm_verdict(" not in queue_source

def test_market_worker_delegates_queue_and_llm_processing_to_runtime() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "runtime" / "market_worker_queue_processing_runtime.py"
    ).read_text(encoding="utf-8")
    risk_source = (
        ROOT / "runtime" / "market_worker_risk_verdict_runtime.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    worker_source = (ROOT / "runtime" / "market_worker.py").read_text(encoding="utf-8")
    worker_tree = ast.parse(worker_source, filename="runtime/market_worker.py")
    imported = {
        alias.name
        for node in worker_tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "runtime.market_worker_processing_runtime"
        for alias in node.names
    }
    assert imported == {"MarketWorkerProcessingRuntime"}

    worker = next(
        node for node in worker_tree.body
        if isinstance(node, ast.ClassDef) and node.name == "MarketWorker"
    )
    start_processing = next(
        node for node in worker.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "start_processing"
    )
    worker_method_source = ast.get_source_segment(worker_source, start_processing) or ""
    assert "await self.processing_runtime.start_processing()" in worker_method_source
    assert "async def process_next_item(" in runtime_source
    assert "anomaly_queue.task_done()" in runtime_source
    assert "_release_ai_ticker(pending_ticker)" in runtime_source
    assert "async def publish_pre_llm_risk_block(" in risk_source
    assert "async def publish_pre_llm_risk_block_locked(" in risk_source


def test_processing_runtime_preserves_llm_context_and_response_contract_delegation() -> None:
    source = (ROOT / "runtime" / "market_worker_queue_processing_runtime.py").read_text(encoding="utf-8")
    context_source = (ROOT / "strategy" / "llm_context.py").read_text(encoding="utf-8")
    response_source = (ROOT / "strategy" / "llm_response.py").read_text(encoding="utf-8")
    assert "await attach_llm_rule_context(" in source
    assert "process_llm_response(" in source
    assert "TechnicalKnowledgeParser" not in source
    assert "contract_retry_attempted" not in source
    assert "model_contract_violation = bool(" not in source
    assert "select_prompt_rules" not in source
    assert "select_prompt_rules" in context_source
    assert "APPROVE/HOLD" in response_source



def test_processing_runtime_delegates_post_llm_evidence_refresh_and_validation() -> None:
    source = (ROOT / "runtime" / "market_worker_queue_processing_runtime.py").read_text(encoding="utf-8")
    module_source = (
        ROOT / "strategy" / "post_llm_validation.py"
    ).read_text(encoding="utf-8")
    assert "validate_post_llm_entry(" in source
    assert "refresh_post_llm_evidence(" not in source
    assert "validate_entry_evidence(" not in source
    assert "classify_direction_evidence(" not in source
    assert "refresh_post_llm_evidence(" in module_source
    assert "validate_entry_evidence(" in module_source
    assert "queue_flow_conflict_reason" in module_source


def test_processing_runtime_delegates_final_entry_readiness_gates() -> None:
    source = (ROOT / "runtime" / "market_worker_queue_processing_runtime.py").read_text(encoding="utf-8")
    module_source = (
        ROOT / "strategy" / "entry_decision_pipeline.py"
    ).read_text(encoding="utf-8")
    assert "entry_plan = prepare_entry_execution_plan(" in source
    assert "calculate_atr_stop(" not in source
    assert "calculate_lot_size(" not in source
    assert "confidence < 75.0" not in source
    assert "confidence >= 75.0" not in source
    assert "_apply_post_llm_evidence_gate" in module_source
    assert "_apply_confidence_gate" in module_source
    assert "_prepare_atr_protected_size" in module_source
    assert "confidence >= 75.0" in module_source


def test_main_delegates_paper_research_routes_to_interface() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "runtime" / "paper_operations_runtime.py"
    ).read_text(encoding="utf-8")
    assert '"paper_operations_runtime": paper_ops' in _compatibility_exports_source()
    assert "PaperOperationsRuntime(" in (ROOT / "runtime" / "application_composition_runtime.py").read_text(encoding="utf-8")
    assert "from interface.paper_research_routes import create_paper_research_router" in runtime_source
    assert "create_paper_research_router(self.paper_research_runtime)" in runtime_source

    legacy_handlers = {
        node.name for node in ast.parse(source, filename="main.py").body
        if isinstance(node, ast.AsyncFunctionDef)
    }
    assert not {
        "get_paper_performance",
        "replay_paper_exit_batch",
        "get_paper_shadow_reconciliation",
        "get_paper_shadow_walk_forward",
        "get_paper_shadow_cost_sensitivity",
        "get_paper_research_report",
        "replay_paper_exit_scenarios",
        "get_paper_reconciliation",
        "reconcile_paper_position",
    } & legacy_handlers


def test_main_delegates_paper_session_maintenance_to_storage_and_interface() -> None:
    main_source = (ROOT / "main.py").read_text(encoding="utf-8")
    composition_source = (
        ROOT / "runtime" / "paper_operations_runtime.py"
    ).read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "storage" / "paper_session_maintenance_runtime.py"
    ).read_text(encoding="utf-8")
    clear_source = (
        ROOT / "storage" / "paper_session_clear.py"
    ).read_text(encoding="utf-8")
    stale_cleanup_source = (
        ROOT / "storage" / "paper_session_stale_cleanup.py"
    ).read_text(encoding="utf-8")
    resume_source = (
        ROOT / "storage" / "paper_session_resume.py"
    ).read_text(encoding="utf-8")
    maintenance_base_source = (
        ROOT / "storage" / "paper_session_maintenance_base.py"
    ).read_text(encoding="utf-8")
    routes_source = (
        ROOT / "interface" / "paper_session_routes.py"
    ).read_text(encoding="utf-8")

    assert "PaperSessionMaintenanceRuntime(" in composition_source
    assert '"paper_session_maintenance_runtime": paper_ops.paper_session_maintenance_runtime' in _compatibility_exports_source()
    assert "create_paper_session_router(" in composition_source
    assert "PAPER_CLEAR_CONFIRMATION" in composition_source

    runtime_tree = ast.parse(runtime_source, filename="storage/paper_session_maintenance_runtime.py")
    runtime_class = next(
        node for node in runtime_tree.body
        if isinstance(node, ast.ClassDef)
        and node.name == "PaperSessionMaintenanceRuntime"
    )
    methods = {
        node.name: node
        for node in runtime_class.body
        if isinstance(node, ast.AsyncFunctionDef)
    }
    assert {
        "clear_paper_session",
        "cleanup_stale_and_resume",
        "resume_paper_session",
    } <= methods.keys()
    assert "async def clear_paper_session(" in clear_source
    assert "clear_unreconciled_paper_positions" in stale_cleanup_source
    assert "priority-zero emergency commands" in maintenance_base_source
    assert 'await self._persist_maintenance(active=False, reason="")' in stale_cleanup_source
    assert 'await self._persist_maintenance(active=False, reason="")' in resume_source

    route_tree = ast.parse(routes_source, filename="interface/paper_session_routes.py")
    route_paths = {
        decorator.args[0].value
        for node in ast.walk(route_tree)
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
        for decorator in node.decorator_list
        if isinstance(decorator, ast.Call)
        and isinstance(decorator.func, ast.Attribute)
        and decorator.func.attr == "post"
        and decorator.args
        and isinstance(decorator.args[0], ast.Constant)
    }
    assert route_paths == {
        "/api/v1/paper/session/clear",
        "/api/v1/paper/session/cleanup-stale-and-resume",
        "/api/v1/paper/session/resume",
    }
    assert "await runtime.clear_paper_session(payload)" in routes_source
    assert "await runtime.cleanup_stale_and_resume()" in routes_source
    assert "await runtime.resume_paper_session()" in routes_source


def test_main_delegates_background_market_services_to_runtime() -> None:
    root = Path(__file__).resolve().parents[1]
    source = (root / "main.py").read_text(encoding="utf-8")
    composition_source = (
        root / "runtime" / "market_application_runtime.py"
    ).read_text(encoding="utf-8")
    runtime_source = (root / "runtime" / "market_background_services.py").read_text(encoding="utf-8")
    finam_source = (root / "runtime" / "market_background_finam_runtime.py").read_text(encoding="utf-8")
    dashboard_source = (root / "runtime" / "market_background_dashboard_runtime.py").read_text(encoding="utf-8")
    assert "create_market_background_services(" in composition_source
    assert "self, MarketBackgroundServicesRuntime" in composition_source
    background_composition_source = (
        root / "runtime" / "market_application_background_composition.py"
    ).read_text(encoding="utf-8")
    assert "services_cls(" in background_composition_source
    assert '"market_background_services": background' in _compatibility_exports_source()
    assert '"background_broker_init": background.background_broker_init' in _compatibility_exports_source()
    assert '"_start_news_updater": background._start_news_updater' in _compatibility_exports_source()
    assert '"_finam_metadata_refresh_loop": background._finam_metadata_refresh_loop' in _compatibility_exports_source()
    assert '"_start_tables_sync_loop": background._start_tables_sync_loop' in _compatibility_exports_source()
    assert "process_market_event=runtime.process_market_event" in background_composition_source
    assert "MarketBackgroundFinamMixin" in runtime_source
    assert "MarketBackgroundDashboardMixin" in runtime_source
    for method in ("background_broker_init", "_finam_metadata_refresh_loop"):
        assert f"async def {method}(self" in finam_source
    for method in ("_start_news_updater", "_start_tables_sync_loop"):
        assert f"async def {method}(self" in dashboard_source
    assert "asyncio.TaskGroup()" in finam_source
    assert "bridge_queue.task_done()" in finam_source
    assert "await self.process_market_event(raw_event)" in finam_source
    assert "RiskManager.update_finam_lot_sizes(" in finam_source
    assert "await self.manager.broadcast(" in dashboard_source
    assert "await self.refresh_dashboard_balance_snapshot(active_positions)" in dashboard_source


def test_main_delegates_dashboard_http_and_websocket_routes_to_interface() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    composition_source = (
        ROOT / "runtime" / "dashboard_application_runtime.py"
    ).read_text(encoding="utf-8")
    route_source = (ROOT / "interface" / "dashboard_routes.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    bootstrap_source = (
        ROOT / "runtime" / "application_runtime_bootstrap.py"
    ).read_text(encoding="utf-8")
    assert "from runtime.application_core_runtime import ApplicationCoreRuntime" in bootstrap_source
    core_source = (ROOT / "runtime" / "application_core_runtime.py").read_text(encoding="utf-8")
    assert "DashboardApplicationRuntime(" in core_source
    application_source = (ROOT / "runtime" / "application_composition_runtime.py").read_text(encoding="utf-8")
    root_source = (ROOT / "runtime" / "application_composition_root_runtime.py").read_text(encoding="utf-8")
    assert "dashboard_application_runtime.register_routes(" in application_source
    assert "ApplicationCompositionRootRuntime(" in (
        ROOT / "runtime" / "application_runtime_bootstrap.py"
    ).read_text(encoding="utf-8")
    assert "ApplicationCompositionRuntime(" in root_source
    assert "create_dashboard_router(" in composition_source
    assert "app.include_router(router)" in composition_source
    assert "@app.websocket(\"/ws/frontend\")" not in source
    assert "@app.post(\"/api/v1/control/close_all\")" not in source
    assert "@app.post(\"/api/v1/control/risk_limit\")" not in source
    assert "@router.websocket(\"/ws/frontend\")" in route_source
    assert "@router.post(\"/api/v1/control/close_all\")" in route_source
    assert "@router.post(\"/api/v1/control/risk_limit\")" in route_source
    assert "priority_queue_item_factory.create(0, \"CMD_CLOSE_ALL\", {})" in route_source
    assert '"type": "INIT_DATA"' in route_source
    assert "await manager.connect(websocket)" in route_source
    assert "manager.disconnect(websocket)" in route_source

