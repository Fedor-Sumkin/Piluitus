"""Compatibility alias for monitoring.market_logger; preserves one shared module instance."""
import sys
from monitoring.market_logger import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["monitoring.market_logger"]
