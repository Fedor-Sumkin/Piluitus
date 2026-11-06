"""Compatibility alias for strategy.scalping_strategy; preserves one shared module instance."""
import sys
from strategy.scalping_strategy import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["strategy.scalping_strategy"]
