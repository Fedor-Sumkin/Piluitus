"""Compatibility alias for the preserved legacy execution agent."""
import sys
from piluitus.execution.legacy_evolution_agent import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["piluitus.execution.legacy_evolution_agent"]
