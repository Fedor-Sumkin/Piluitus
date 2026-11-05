"""Compatibility alias for piluitus.ai.ai_engine; keeps a single shared module instance."""
import sys
from piluitus.ai.ai_engine import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["piluitus.ai.ai_engine"]
