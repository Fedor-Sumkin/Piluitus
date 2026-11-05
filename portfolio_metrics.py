"""Compatibility alias for piluitus.portfolio.portfolio_metrics; keeps a single shared module instance."""
import sys
from piluitus.portfolio.portfolio_metrics import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["piluitus.portfolio.portfolio_metrics"]
