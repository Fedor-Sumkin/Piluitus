"""Smoke tests for legacy root imports and the new package layout."""

def test_strategy_legacy_imports_are_module_aliases():
    import candidate_recovery
    import minute_bar_patterns
    import signal_guard
    from piluitus.strategy import candidate_recovery as packaged_recovery
    from piluitus.strategy import minute_bar_patterns as packaged_patterns
    from piluitus.strategy import signal_guard as packaged_guard

    assert candidate_recovery is packaged_recovery
    assert minute_bar_patterns is packaged_patterns
    assert signal_guard is packaged_guard


def test_risk_and_portfolio_legacy_imports_are_module_aliases():
    import risk_limits
    import portfolio_metrics
    from piluitus.risk import risk_limits as packaged_limits
    from piluitus.portfolio import portfolio_metrics as packaged_metrics

    assert risk_limits is packaged_limits
    assert portfolio_metrics is packaged_metrics
