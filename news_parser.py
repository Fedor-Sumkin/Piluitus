"""Compatibility alias for ai.news_parser; preserves one shared module instance."""
import sys
from ai.news_parser import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["ai.news_parser"]
