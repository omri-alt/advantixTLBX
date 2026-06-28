#!/usr/bin/env python3
"""CLI wrapper for sk_bulk_bid_raising.py."""
import runpy
from pathlib import Path

if __name__ == "__main__":
    runpy.run_path(str(Path(__file__).resolve().parent.parent / "sk_bulk_bid_raising.py"))
