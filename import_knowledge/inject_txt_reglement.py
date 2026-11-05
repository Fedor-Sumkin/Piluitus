"""Compatibility launcher; implementation moved to tools/knowledge/inject_txt_reglement.py."""
from pathlib import Path
import runpy
_TARGET = Path(__file__).resolve().parents[1] / "tools" / "knowledge" / "inject_txt_reglement.py"
runpy.run_path(str(_TARGET), run_name="__main__")
