"""Compatibility alias for ai.knowledge_parser; preserves one shared module instance."""
import sys
from ai.knowledge_parser import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["ai.knowledge_parser"]
