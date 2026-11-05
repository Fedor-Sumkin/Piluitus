"""Compatibility launcher; implementation moved to tools/knowledge/merge_rules.py."""
from pathlib import Path
import runpy
_TARGET = Path(__file__).resolve().parent / "tools/knowledge/merge_rules.py"
runpy.run_path(str(_TARGET), run_name="__main__")
