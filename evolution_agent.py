"""Compatibility alias for execution.legacy_evolution_agent; preserves one shared module instance."""
import sys
from execution.legacy_evolution_agent import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["execution.legacy_evolution_agent"]
