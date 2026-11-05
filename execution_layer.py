"""Compatibility alias for piluitus.execution.execution_layer; keeps a single shared module instance."""
import sys
from piluitus.execution.execution_layer import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["piluitus.execution.execution_layer"]
