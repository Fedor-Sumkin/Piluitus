"""Compatibility alias for storage.db_manager; preserves one shared module instance."""
import sys
from storage.db_manager import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["storage.db_manager"]
