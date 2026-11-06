"""Compatibility alias for strategy.shadow_outcomes; preserves one shared module instance."""
import sys
from strategy.shadow_outcomes import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["strategy.shadow_outcomes"]
