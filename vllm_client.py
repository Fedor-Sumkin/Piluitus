"""Compatibility alias for ai.vllm_client; preserves one shared module instance."""
import sys
from ai.vllm_client import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["ai.vllm_client"]
