"""Compatibility alias for interface.tg_bot; preserves shared bot globals."""
import sys
from interface.tg_bot import *  # noqa: F401,F403
sys.modules[__name__] = sys.modules["interface.tg_bot"]
