"""Compatibility alias for piluitus.portfolio.pnl_exporter; keeps one module instance."""
import sys
from piluitus.portfolio.pnl_exporter import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["piluitus.portfolio.pnl_exporter"]
