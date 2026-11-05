"""Compatibility alias for ai.ai_engine; preserves one shared module instance."""
import sys
from ai.ai_engine import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["ai.ai_engine"]
