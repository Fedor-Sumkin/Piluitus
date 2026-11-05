"""Compatibility alias for piluitus.ai.news_parser; keeps a single shared module instance."""
import sys
from piluitus.ai.news_parser import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["piluitus.ai.news_parser"]
