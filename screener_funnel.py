"""Compatibility alias for strategy.screener_funnel; preserves one shared module instance."""
import sys
from strategy.screener_funnel import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["strategy.screener_funnel"]
