"""Compatibility alias for portfolio.pnl_exporter; preserves one shared module instance."""
import sys
from portfolio.pnl_exporter import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["portfolio.pnl_exporter"]
