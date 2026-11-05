"""Compatibility alias for piluitus.exchange.finam_client; keeps a single shared module instance."""
import sys
from piluitus.exchange.finam_client import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["piluitus.exchange.finam_client"]
