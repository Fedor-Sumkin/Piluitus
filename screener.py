"""Compatibility alias for strategy.screener; preserves one shared module instance."""
import sys
from strategy.screener import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["strategy.screener"]
