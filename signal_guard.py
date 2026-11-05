"""Compatibility alias for strategy.signal_guard; preserves one shared module instance."""
import sys
from strategy.signal_guard import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["strategy.signal_guard"]
