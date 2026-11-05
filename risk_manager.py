"""Compatibility alias for risk.risk_manager; preserves one shared module instance."""
import sys
from risk.risk_manager import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["risk.risk_manager"]
