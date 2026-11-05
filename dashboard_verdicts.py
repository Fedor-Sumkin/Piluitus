"""Compatibility alias for piluitus.ai.dashboard_verdicts; keeps a single shared module instance."""
import sys
from piluitus.ai.dashboard_verdicts import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["piluitus.ai.dashboard_verdicts"]
