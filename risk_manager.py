"""Compatibility alias for piluitus.risk.risk_manager; keeps a single shared module instance."""
import sys
from piluitus.risk.risk_manager import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["piluitus.risk.risk_manager"]
