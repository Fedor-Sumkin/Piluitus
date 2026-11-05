"""Compatibility alias for piluitus.strategy.screener_funnel; keeps a single shared module instance."""
import sys
from piluitus.strategy.screener_funnel import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["piluitus.strategy.screener_funnel"]
