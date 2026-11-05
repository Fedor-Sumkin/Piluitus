"""Compatibility alias for piluitus.strategy.signal_guard; keeps one shared module instance."""
import sys
from piluitus.strategy.signal_guard import __name__ as _name
from piluitus.strategy.signal_guard import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules[_name]
