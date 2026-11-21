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
        "root-shim dependencies:\n" + "\n".join(violations)
    )


def test_main_uses_canonical_package_imports() -> None:
    main_path = ROOT / "main.py"
    violations = _legacy_imports(main_path.read_text(encoding="utf-8"), "main.py")
    assert not violations, (
        "main.py is the supported root entry point, but its internal imports "
        "must resolve through packages:\n" + "\n".join(violations)
    )

def test_main_reuses_extracted_terminal_status_implementations() -> None:
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="main.py")
    local_classes = {
        node.name for node in tree.body if isinstance(node, ast.ClassDef)
    }
    assert "TerminalStatusLine" not in local_classes
    assert "StatusAwareStreamHandler" not in local_classes

    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "monitoring.terminal_status"
        for alias in node.names
    }
    assert {"TerminalStatusLine", "StatusAwareStreamHandler"} <= imported



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
