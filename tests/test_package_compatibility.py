"""Smoke tests for legacy root imports and the new package layout."""
from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

LEGACY_SHIMS = {
    "ai_engine.py": "piluitus.ai.ai_engine",
    "candidate_recovery.py": "piluitus.strategy.candidate_recovery",
    "dashboard_verdicts.py": "piluitus.ai.dashboard_verdicts",
    "db_manager.py": "piluitus.storage.db_manager",
    "execution_layer.py": "piluitus.execution.execution_layer",
    "finam_client.py": "piluitus.exchange.finam_client",
    "knowledge_parser.py": "piluitus.ai.knowledge_parser",
    "market_logger.py": "piluitus.monitoring.market_logger",
    "minute_bar_patterns.py": "piluitus.strategy.minute_bar_patterns",
    "news_parser.py": "piluitus.ai.news_parser",
    "pnl_exporter.py": "piluitus.portfolio.pnl_exporter",
    "portfolio_metrics.py": "piluitus.portfolio.portfolio_metrics",
    "rag_prompt_utils.py": "piluitus.ai.rag_prompt_utils",
    "risk_limits.py": "piluitus.risk.risk_limits",
    "risk_manager.py": "piluitus.risk.risk_manager",
    "screener.py": "piluitus.strategy.screener",
    "screener_funnel.py": "piluitus.strategy.screener_funnel",
    "signal_guard.py": "piluitus.strategy.signal_guard",
    "tg_bot.py": "interface.tg_bot",
    "vllm_client.py": "piluitus.ai.vllm_client",
}


def test_all_legacy_root_shims_target_the_expected_package_module() -> None:
    for root_file, target_module in LEGACY_SHIMS.items():
        source = (PROJECT_ROOT / root_file).read_text(encoding="utf-8")
        assert f"from {target_module} import *" in source, root_file

        direct_alias = f'sys.modules[__name__] = sys.modules["{target_module}"]'
        indirect_alias = (
            f"from {target_module} import __name__ as _name"
            "\n"
            "sys.modules[__name__] = sys.modules[_name]"
        )
        assert direct_alias in source or indirect_alias in source, root_file


def test_strategy_legacy_imports_are_module_aliases() -> None:
    import candidate_recovery
    import minute_bar_patterns
    import signal_guard
    from piluitus.strategy import candidate_recovery as packaged_recovery
    from piluitus.strategy import minute_bar_patterns as packaged_patterns
    from piluitus.strategy import signal_guard as packaged_guard

    assert candidate_recovery is packaged_recovery
    assert minute_bar_patterns is packaged_patterns
    assert signal_guard is packaged_guard


def test_risk_and_portfolio_legacy_imports_are_module_aliases() -> None:
    import risk_limits
    import portfolio_metrics
    from piluitus.risk import risk_limits as packaged_limits
    from piluitus.portfolio import portfolio_metrics as packaged_metrics

    assert risk_limits is packaged_limits
    assert portfolio_metrics is packaged_metrics
