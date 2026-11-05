"""Compatibility alias for execution.execution_layer; preserves one shared module instance."""
import sys
from execution.execution_layer import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["execution.execution_layer"]
