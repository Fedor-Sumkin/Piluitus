"""Compatibility alias for exchange.finam_client; preserves one shared module instance."""
import sys
from exchange.finam_client import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["exchange.finam_client"]
