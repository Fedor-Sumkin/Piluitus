"""Compatibility alias for piluitus.storage.db_manager; keeps a single shared module instance."""
import sys
from piluitus.storage.db_manager import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["piluitus.storage.db_manager"]
