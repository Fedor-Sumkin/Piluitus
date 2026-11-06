"""Smoke tests for legacy root imports and the new package layout."""
from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

LEGACY_SHIMS = {
    "ai_engine.py": "ai.ai_engine",
    "candidate_recovery.py": "strategy.candidate_recovery",
    "dashboard_verdicts.py": "ai.dashboard_verdicts",
    "db_manager.py": "storage.db_manager",
    "execution_layer.py": "execution.execution_layer",
    "evolution_agent.py": "execution.legacy_evolution_agent",
    "finam_client.py": "exchange.finam_client",
    "knowledge_parser.py": "ai.knowledge_parser",
    "market_logger.py": "monitoring.market_logger",
    "minute_bar_patterns.py": "strategy.minute_bar_patterns",
    "news_parser.py": "ai.news_parser",
    "pnl_exporter.py": "portfolio.pnl_exporter",
    "portfolio_metrics.py": "portfolio.portfolio_metrics",
    "rag_prompt_utils.py": "ai.rag_prompt_utils",
    "risk_limits.py": "risk.risk_limits",
    "risk_manager.py": "risk.risk_manager",
    "screener.py": "strategy.screener",
    "screener_funnel.py": "strategy.screener_funnel",
    "scalping_strategy.py": "strategy.scalping_strategy",
    "signal_guard.py": "strategy.signal_guard",
    "tg_bot.py": "interface.tg_bot",
    "vllm_client.py": "ai.vllm_client",
}


def test_all_legacy_root_shims_target_the_expected_package_module() -> None:
    for root_file, target_module in LEGACY_SHIMS.items():
        source = (PROJECT_ROOT / root_file).read_text(encoding="utf-8")
        assert f"from {target_module} import *" in source, root_file

        direct_alias = f'sys.modules[__name__] = sys.modules["{target_module}"]'
        indirect_alias = (
            f"from {target_module} import __name__ as _name" in source
            and "sys.modules[__name__] = sys.modules[_name]" in source
        )
        assert direct_alias in source or indirect_alias, root_file


def test_strategy_legacy_imports_are_module_aliases() -> None:
    import candidate_recovery
    import minute_bar_patterns
    import scalping_strategy
    import signal_guard
    from strategy import candidate_recovery as packaged_recovery
    from strategy import minute_bar_patterns as packaged_patterns
    from strategy import scalping_strategy as packaged_scalping
    from strategy import signal_guard as packaged_guard

    assert candidate_recovery is packaged_recovery
    assert minute_bar_patterns is packaged_patterns
    assert scalping_strategy is packaged_scalping
    assert signal_guard is packaged_guard


def test_risk_and_portfolio_legacy_imports_are_module_aliases() -> None:
    import risk_limits
    import portfolio_metrics
    from risk import risk_limits as packaged_limits
    from portfolio import portfolio_metrics as packaged_metrics

    assert risk_limits is packaged_limits
    assert portfolio_metrics is packaged_metrics


def test_legacy_evolution_agent_root_import_is_module_alias() -> None:
    import evolution_agent
    from execution import legacy_evolution_agent

    assert evolution_agent is legacy_evolution_agent
