"""Compatibility alias for piluitus.strategy.candidate_recovery; keeps one shared module instance."""
import sys
from piluitus.strategy.candidate_recovery import __name__ as _name
from piluitus.strategy.candidate_recovery import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules[_name]
