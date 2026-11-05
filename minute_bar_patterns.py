"""Compatibility alias for piluitus.strategy.minute_bar_patterns; keeps one shared module instance."""
import sys
from piluitus.strategy.minute_bar_patterns import __name__ as _name
from piluitus.strategy.minute_bar_patterns import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules[_name]
