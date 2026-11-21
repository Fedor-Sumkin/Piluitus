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
    assert "position_order_book_diagnostics.clear()" in source



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
