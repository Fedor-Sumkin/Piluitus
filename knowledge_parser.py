"""Compatibility alias for piluitus.ai.knowledge_parser; keeps one module instance."""
import sys
from piluitus.ai.knowledge_parser import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["piluitus.ai.knowledge_parser"]
