"""Compatibility alias for piluitus.ai.rag_prompt_utils; keeps a single shared module instance."""
import sys
from piluitus.ai.rag_prompt_utils import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["piluitus.ai.rag_prompt_utils"]
