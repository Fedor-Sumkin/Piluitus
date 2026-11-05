"""Compatibility alias for strategy.candidate_recovery; preserves one shared module instance."""
import sys
from strategy.candidate_recovery import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["strategy.candidate_recovery"]
