"""Guard the package migration against accidental legacy imports.

Root-level compatibility modules intentionally remain available for external
scripts and older tests. Application packages must import their canonical
package modules directly.
"""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
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

def test_main_imports_reason_classifiers_from_monitoring() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imports = {
        alias.name: alias.asname
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.trade_flow_diagnostics"
        for alias in node.names
    }
    assert imports == {
        "TradeFlowCounters": None,
        "bbo_resume_reason_bucket": "_bbo_resume_reason_bucket",
        "guard_reason_bucket": "_guard_reason_bucket",
    }


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


def test_main_delegates_trade_flow_state_to_monitoring() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.trade_flow_diagnostics"
        for alias in node.names
    }
    assert "TradeFlowCounters" in imported
    assert "_trade_flow_counters = TradeFlowCounters(time_module.monotonic())" in source
    assert "_trade_flow_lock" not in source
    assert "_trade_flow_counts" not in source
    assert "_trade_flow_last_log_at" not in source



def test_main_reuses_extracted_websocket_manager() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    local_classes = {
        node.name for node in tree.body if isinstance(node, ast.ClassDef)
    }
    assert "ConnectionManager" not in local_classes

    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "interface.websocket_manager"
        for alias in node.names
    }
    assert "ConnectionManager" in imported

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

    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.logging_setup"
        for alias in node.names
    }
    assert {
        "RUN_LOG_PATHS",
        "logger",
        "terminal_status",
        "update_numpy_impulse_status",
    } <= imported

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



def test_main_delegates_background_operation_tracking_to_monitoring() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imports = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.background_tasks"
        for alias in node.names
    }
    assert "BackgroundOperationTracker" in imports
    assert "_background_operations = BackgroundOperationTracker(logger)" in source
    assert "_tracked_operation_tasks" not in source
    assert "def _spawn_tracked_operation" not in source
    assert "async def _drain_tracked_operations" not in source



def test_main_delegates_trading_event_journal_to_monitoring() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imports = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.trading_event_logger"
        for alias in node.names
    }
    assert "log_trading_event_to_file" in imports
    assert "async def log_trading_event_to_file" not in source
    assert "_sync_write_trading_log" not in source



def test_main_delegates_position_order_book_diagnostics_to_monitoring() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "storage" / "paper_session_maintenance_runtime.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imports = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.position_order_book_diagnostics"
        for alias in node.names
    }
    assert "PositionOrderBookDiagnostics" in imports
    assert "position_order_book_diagnostics = PositionOrderBookDiagnostics(logger)" in source
    assert "_position_order_book_diag_lock" not in source
    assert "_position_order_book_diag:" not in source
    assert "position_order_book_diagnostics.clear()" in runtime_source



def test_main_delegates_prefilter_warning_rate_limiting_to_monitoring() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imports = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.prefilter_diagnostics"
        for alias in node.names
    }
    assert "PrefilterBlockLogger" in imports
    assert "prefilter_block_logger = PrefilterBlockLogger(logger)" in source
    assert "_prefilter_last_logged" not in source
    assert "_prefilter_log_lock" not in source



def test_main_delegates_strategy_shadow_scorecard_logging_to_monitoring() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imports = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.shadow_scorecard_logger"
        for alias in node.names
    }
    assert "StrategyShadowScorecardLogger" in imports
    assert "shadow_scorecard_logger = StrategyShadowScorecardLogger(logger)" in source
    assert "shadow_scorecard_logger.log(scalping_shadow_tracker)" in source
    assert "scalping_shadow_tracker.statistics_snapshot()" not in source


def test_main_delegates_trade_flow_report_formatting_to_monitoring() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imports = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.trade_flow_report"
        for alias in node.names
    }
    assert "TradeFlowReportLogger" in imports
    assert "trade_flow_report_logger = TradeFlowReportLogger(logger)" in source
    assert "trade_flow_report_logger.log(counts, pending_bbo=pending_bbo)" in source



def test_main_delegates_strategy_shadow_events_to_monitoring() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.strategy_shadow_diagnostics"
        for alias in node.names
    }
    assert imported == {"StrategyShadowDiagnostics"}
    assert "strategy_shadow_diagnostics.mark_llm_not_called(candidate_id, reason_code)" in source
    assert "strategy_shadow_diagnostics.log_candidate(ticker, direction, metrics)" in source
    shadow_wrappers = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_log_scalping_shadow"
    ]
    assert len(shadow_wrappers) == 1
    body = [
        node for node in shadow_wrappers[0].body
        if not (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        )
    ]
    assert len(body) == 1 and isinstance(body[0], ast.Return)
    assert ast.unparse(body[0].value) == (
        "strategy_shadow_diagnostics.log_candidate(ticker, direction, metrics)"
    )



def test_main_delegates_priority_queue_item_creation_to_runtime() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "runtime.priority_queue"
        for alias in node.names
    }
    assert imported == {"PriorityItem", "PriorityQueueItemFactory"}
    assert "priority_queue_item_factory = PriorityQueueItemFactory()" in source
    assert "def _make_priority_item" not in source
    assert "_priority_sequence" not in source
    assert "from itertools import count" not in source



def test_main_delegates_paper_open_pnl_accounting_to_portfolio() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    runtime_imports = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "portfolio.dashboard_runtime"
        for alias in node.names
    }
    assert runtime_imports == {"DashboardRuntime"}
    accounting_imports = {
        alias.name: alias.asname
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "portfolio.paper_accounting"
        for alias in node.names
    }
    assert accounting_imports == {"parse_finite_float": "_parse_finite_float"}
    assert "dashboard_runtime.current_paper_open_pnl()" in source
    assert "dashboard_runtime.current_paper_assets_cost()" in source


def test_main_delegates_dashboard_balance_calculation_to_portfolio() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "portfolio.dashboard_accounting"
        for alias in node.names
    }
    assert imported == {"build_dashboard_balance_snapshot"}
    assert "calculation = build_dashboard_balance_snapshot(" in source
    refresh = next(
        node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "_refresh_dashboard_balance_snapshot"
    )
    refresh_source = ast.get_source_segment(source, refresh) or ""
    assert "calculate_assets_used(" not in refresh_source


def test_main_delegates_shadow_outcome_persistence_to_storage() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "storage.shadow_outcomes"
        for alias in node.names
    }
    assert imported == {
        "ShadowOutcomePersistence",
        "SHADOW_OUTCOMES_STORAGE_LIMIT",
    }
    assert "shadow_outcome_persistence = ShadowOutcomePersistence(" in source
    assert "paper_session_runtime = PaperSessionRuntime(" in source

    persist = next(
        node for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "_persist_shadow_outcomes"
    )
    restore = next(
        node for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "_restore_shadow_outcomes"
    )
    assert "paper_session_runtime.persist_shadow_outcomes(labels, source=source)" in (
        ast.get_source_segment(source, persist) or ""
    )
    assert "paper_session_runtime.restore_shadow_outcomes()" in (
        ast.get_source_segment(source, restore) or ""
    )
    assert "persist_completed_labels(" not in (ast.get_source_segment(source, persist) or "")
    assert "restore_completed_labels(" not in (ast.get_source_segment(source, restore) or "")


def test_main_delegates_market_data_runtime_services() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "strategy.market_data_runtime"
        for alias in node.names
    }
    assert {"M1HistoryWarmup", "PendingBBOCandidateStore", "paper_queue_flow_conflict_reason"} <= imported
    assert "m1_history_warmup = M1HistoryWarmup(" in source
    assert "pending_bbo_candidate_store = PendingBBOCandidateStore(" in source
    assert "coalesce_pending_bbo_entry(" not in source
    assert "bars = await broker.get_historical_m1_bars(" not in source


def test_main_delegates_paper_api_helpers_to_interface() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    assert "paper_api_helpers = PaperAPIHelpers(" in source
    assert "paper_api_helpers.require_paper_mode()" in source
    assert "paper_api_helpers.paper_trade_rows(snapshot)" in source
    assert "paper_api_helpers.broadcast_paper_tables()" in source


def test_main_delegates_paper_research_and_reconciliation_apis_to_storage() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    router_source = (
        ROOT / "interface" / "paper_research_routes.py"
    ).read_text(encoding="utf-8")
    main_tree = ast.parse(source, filename="main.py")
    router_tree = ast.parse(router_source, filename="paper_research_routes.py")

    runtime_imports = {
        alias.name
        for node in main_tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "storage.paper_research_runtime"
        for alias in node.names
    }
    assert runtime_imports == {"PaperResearchRuntime"}
    assert "paper_research_runtime = PaperResearchRuntime(" in source

    router_imports = {
        alias.name
        for node in main_tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "interface.paper_research_routes"
        for alias in node.names
    }
    assert router_imports == {"create_paper_research_router"}
    assert (
        "app.include_router(create_paper_research_router(paper_research_runtime))"
        in source
    )

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


def test_main_uses_canonical_shadow_retention_limit() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "storage.shadow_outcomes"
        for alias in node.names
    }
    assert {"ShadowOutcomePersistence", "SHADOW_OUTCOMES_STORAGE_LIMIT"} <= imported
    assert "shadow_outcomes_storage_limit=SHADOW_OUTCOMES_STORAGE_LIMIT" in source


def test_lifespan_delegates_long_running_task_ownership_to_supervisor() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imports = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.background_tasks"
        for alias in node.names
    }
    assert {"BackgroundOperationTracker", "BackgroundTaskSupervisor"} <= imports
    lifespan = next(
        node for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "lifespan"
    )
    lifespan_source = ast.get_source_segment(source, lifespan) or ""
    assert "BackgroundTaskSupervisor(logger)" in lifespan_source
    assert "background_task_supervisor.spawn(" in lifespan_source
    assert "background_task_supervisor.cancel_and_drain()" in lifespan_source
    assert "_spawn_background" not in lifespan_source
    assert "background_tasks: set[asyncio.Task]" not in lifespan_source

def test_main_delegates_market_event_normalization_to_runtime() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "runtime" / "market_data_event_runtime.py"
    ).read_text(encoding="utf-8")
    normalizer_source = (
        ROOT / "runtime" / "market_event_normalizer.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "runtime.market_data_event_runtime"
        for alias in node.names
    }
    assert imported == {"MarketDataEventRuntime"}
    assert "market_data_event_runtime = MarketDataEventRuntime(" in source
    assert "await market_data_event_runtime.process_event(event)" in source
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
    assert "MarketAuxiliaryEventRouter(" in source
    assert "if await self.auxiliary_event_router.process_if_handled(event):" in runtime_source
    assert "self.auxiliary_event_router.bbo_replay.record(" in runtime_source
    assert "await self.screener.push_order_book(**normalized)" in router_source
    assert "await self.pending_bbo_runtime.resume(ticker, trigger=\"QUOTES\")" in runtime_source


def test_main_delegates_candidate_admission_to_strategy_pipeline() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "runtime" / "market_data_event_runtime.py"
    ).read_text(encoding="utf-8")
    pipeline_source = (
        ROOT / "strategy" / "market_candidate_pipeline.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "strategy.market_candidate_pipeline"
        for alias in node.names
    }
    assert imported == {"MarketCandidatePipeline"}
    assert "market_candidate_pipeline = MarketCandidatePipeline(" in source
    assert "self.candidate_pipeline.process_candidate(" in runtime_source
    assert "validate_atr_readiness" not in runtime_source
    assert "infer_candidate_direction" not in runtime_source
    assert "self.infer_direction(metrics)" in pipeline_source
    assert "validate_entry_evidence" in pipeline_source
    assert "validate_atr_readiness" in pipeline_source
    assert "anomaly_queue.put_nowait" in pipeline_source


def test_main_delegates_pending_bbo_candidate_lifecycle_to_strategy_runtime() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    runtime_source = (
        ROOT / "strategy" / "market_data_runtime.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "strategy.market_data_runtime"
        for alias in node.names
    }
    assert "PendingBBOCandidateRuntime" in imported
    assert "pending_bbo_runtime = PendingBBOCandidateRuntime(" in source
    assert "pending_bbo_runtime.has_pending(ticker)" in source
    assert "pending_bbo_candidate_store.count()" in source

    wrapper = next(
        node for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "_resume_pending_bbo_candidate"
    )
    wrapper_source = ast.get_source_segment(source, wrapper) or ""
    assert "await pending_bbo_runtime.resume(ticker, trigger=trigger)" in wrapper_source
    assert "refresh_candidate_bbo_metrics" not in wrapper_source
    assert "validate_entry_evidence" not in wrapper_source
    assert "validate_atr_readiness" not in wrapper_source

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


def test_main_delegates_execution_fill_normalization_to_execution_package() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    module_source = (ROOT / "execution" / "outcome_reporting.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "execution.outcome_reporting"
        for alias in node.names
    }
    assert imported == {"normalize_execution_fill"}
    assert "execution_fill = normalize_execution_fill(" in source
    assert "actual_lots = nonnegative_int(raw_actual_lots)" not in source
    assert "actual_shares = actual_lots *" not in source
    assert "executor_requested_lots = nonnegative_int(result.get(\"requested_lots\"))" in module_source




def test_main_delegates_execution_outcome_publishing_to_execution_package() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "execution.outcome_publisher"
        for alias in node.names
    }
    assert imported == {"publish_execution_outcome"}
    assert "await publish_execution_outcome(" in source
    worker = source[source.index("class MarketWorker"):source.index("#  6️⃣")]
    assert "await manager.broadcast(json.dumps({\"type\": \"EXECUTION_UPDATE\"" not in worker


def test_main_delegates_market_verdict_publication_to_execution_package() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    module_source = (
        ROOT / "execution" / "verdict_publisher.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "execution.verdict_publisher"
        for alias in node.names
    }
    assert imported == {"publish_market_verdict"}
    assert "await publish_market_verdict(" in source
    worker_class = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "MarketWorker"
    )
    start_processing = next(
        node for node in worker_class.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "start_processing"
    )
    start_processing_source = ast.get_source_segment(source, start_processing) or ""
    assert "redis_inst.lpush(" not in start_processing_source
    assert "manager.broadcast(json.dumps(payload" not in start_processing_source
    assert "verdict_cache.append(verdict)" in module_source


def test_main_delegates_verdict_record_construction_and_enrichment_to_strategy() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    module_source = (
        ROOT / "strategy" / "verdict_records.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "strategy.verdict_records"
        for alias in node.names
    }
    assert imported == {
        "build_pre_llm_risk_block_verdict",
        "build_default_signal_reason",
        "classify_local_evidence_diagnostic",
        "build_llm_verdict_record",
        "link_shadow_candidate_verdict",
        "enrich_ai_analysis_with_verdict",
    }
    assert "verdict = build_pre_llm_risk_block_verdict(" in source
    assert "verdict_data = build_llm_verdict_record(" in source
    assert "link_shadow_candidate_verdict(" in source
    assert "enrich_ai_analysis_with_verdict(" in source
    assert "decision_source" in module_source
    assert "llm_reason_code_raw" in module_source
    assert "market_regime" in module_source
    assert "scalping_shadow_tracker.attach_llm_verdict(" not in source



def test_market_worker_delegates_llm_rule_context_and_response_contract() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    context_source = (ROOT / "strategy" / "llm_context.py").read_text(encoding="utf-8")
    response_source = (ROOT / "strategy" / "llm_response.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module in {"strategy.llm_context", "strategy.llm_response"}
        for alias in node.names
    }
    assert imported >= {
        "attach_llm_rule_context",
        "build_llm_error_response",
        "process_llm_response",
    }
    worker = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "MarketWorker"
    )
    start_processing = next(
        node for node in worker.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "start_processing"
    )
    worker_source = ast.get_source_segment(source, start_processing) or ""
    assert "await attach_llm_rule_context(" in worker_source
    assert "process_llm_response(" in worker_source
    assert "TechnicalKnowledgeParser" not in worker_source
    assert "contract_retry_attempted" not in worker_source
    # The normalized flag is intentionally still consumed by later diagnostics;
    # its parsing and mutation now live in the response-pipeline module.
    assert "model_contract_violation = bool(" not in worker_source
    assert "select_prompt_rules" not in source
    assert "select_prompt_rules" in context_source
    assert "APPROVE/HOLD" in response_source


def test_market_worker_delegates_post_llm_evidence_refresh_and_validation() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    module_source = (
        ROOT / "strategy" / "post_llm_validation.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "strategy.post_llm_validation"
        for alias in node.names
    }
    assert imported == {"validate_post_llm_entry"}
    worker = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "MarketWorker"
    )
    start_processing = next(
        node for node in worker.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "start_processing"
    )
    worker_source = ast.get_source_segment(source, start_processing) or ""
    assert "validate_post_llm_entry(" in worker_source
    assert "refresh_post_llm_evidence(" not in worker_source
    assert "validate_entry_evidence(" not in worker_source
    assert "classify_direction_evidence(" not in worker_source
    assert "refresh_post_llm_evidence(" in module_source
    assert "validate_entry_evidence(" in module_source
    assert "queue_flow_conflict_reason" in module_source


def test_main_delegates_post_llm_entry_and_execution_readiness_gates() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    module_source = (
        ROOT / "strategy" / "entry_decision_pipeline.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "strategy.entry_decision_pipeline"
        for alias in node.names
    }
    assert imported == {"prepare_entry_execution_plan"}

    worker = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "MarketWorker"
    )
    start_processing = next(
        node for node in worker.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "start_processing"
    )
    worker_source = ast.get_source_segment(source, start_processing) or ""
    assert "entry_plan = prepare_entry_execution_plan(" in worker_source
    assert "calculate_atr_stop(" not in worker_source
    assert "calculate_lot_size(" not in worker_source
    assert "confidence < 75.0" not in worker_source
    assert "confidence >= 75.0" not in worker_source
    assert "_apply_post_llm_evidence_gate" in module_source
    assert "_apply_confidence_gate" in module_source
    assert "_prepare_atr_protected_size" in module_source
    assert "confidence >= 75.0" in module_source



def test_main_delegates_paper_research_routes_to_interface() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "interface.paper_research_routes"
        for alias in node.names
    }
    assert imported == {"create_paper_research_router"}
    assert "app.include_router(create_paper_research_router(paper_research_runtime))" in source

    legacy_handlers = {
        node.name for node in tree.body
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
    runtime_source = (
        ROOT / "storage" / "paper_session_maintenance_runtime.py"
    ).read_text(encoding="utf-8")
    routes_source = (
        ROOT / "interface" / "paper_session_routes.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(main_source, filename="main.py")

    runtime_imports = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "storage.paper_session_maintenance_runtime"
        for alias in node.names
    }
    route_imports = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "interface.paper_session_routes"
        for alias in node.names
    }
    assert runtime_imports == {
        "PAPER_CLEAR_CONFIRMATION",
        "PaperSessionMaintenanceRuntime",
    }
    assert route_imports == {"create_paper_session_router"}
    assert "paper_session_maintenance_runtime = PaperSessionMaintenanceRuntime(" in main_source
    compact_main_source = "".join(main_source.split())
    assert (
        "app.include_router("
        "create_paper_session_router(paper_session_maintenance_runtime)"
        ")" in compact_main_source
    )

    legacy_handlers = {
        node.name for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
    }
    assert not {
        "clear_paper_session",
        "cleanup_stale_and_resume_paper_session",
        "resume_paper_session",
    } & legacy_handlers

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
    assert "clear_unreconciled_paper_positions" in runtime_source
    assert "clear_paper_session()" in runtime_source
    assert "Priority-zero emergency commands are retained" in runtime_source
    assert 'await self._persist_maintenance(active=False, reason="")' in runtime_source

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
