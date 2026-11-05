"""Compatibility alias for piluitus.monitoring.market_logger; keeps one module instance."""
import sys
from piluitus.monitoring.market_logger import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["piluitus.monitoring.market_logger"]
