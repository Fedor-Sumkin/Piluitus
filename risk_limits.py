"""Compatibility alias for risk.risk_limits; preserves one shared module instance."""
import sys
from risk.risk_limits import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["risk.risk_limits"]
