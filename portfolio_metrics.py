"""Compatibility alias for portfolio.portfolio_metrics; preserves one shared module instance."""
import sys
from portfolio.portfolio_metrics import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["portfolio.portfolio_metrics"]
