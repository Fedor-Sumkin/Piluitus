"""Compatibility alias for strategy.minute_bar_patterns; preserves one shared module instance."""
import sys
from strategy.minute_bar_patterns import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["strategy.minute_bar_patterns"]
