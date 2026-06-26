#!/usr/bin/env python3
"""CLI wrapper for run_keitaro_sync_v2.py."""
import runpy
from pathlib import Path

if __name__ == "__main__":
    runpy.run_path(str(Path(__file__).resolve().parent.parent / "run_keitaro_sync_v2.py"))
