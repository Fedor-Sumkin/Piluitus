"""Compatibility launcher; implementation moved to tools/data/historical_downloader.py."""
from pathlib import Path
import runpy
_TARGET = Path(__file__).resolve().parent / "tools/data/historical_downloader.py"
runpy.run_path(str(_TARGET), run_name="__main__")
