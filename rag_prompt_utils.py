"""Compatibility alias for ai.rag_prompt_utils; preserves one shared module instance."""
import sys
from ai.rag_prompt_utils import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["ai.rag_prompt_utils"]
