"""Compatibility alias for ai.dashboard_verdicts; preserves one shared module instance."""
import sys
from ai.dashboard_verdicts import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["ai.dashboard_verdicts"]
