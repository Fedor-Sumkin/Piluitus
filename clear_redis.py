"""Compatibility launcher; implementation moved to tools/maintenance/clear_redis.py."""
from pathlib import Path
import runpy
_TARGET = Path(__file__).resolve().parent / "tools/maintenance/clear_redis.py"
runpy.run_path(str(_TARGET), run_name="__main__")
