"""Compatibility alias for piluitus.risk.risk_limits; keeps a single shared module instance."""
import sys
from piluitus.risk.risk_limits import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["piluitus.risk.risk_limits"]
