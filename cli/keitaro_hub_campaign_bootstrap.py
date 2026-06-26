#!/usr/bin/env python3
"""CLI wrapper for scripts/keitaro_hub_campaign_bootstrap.py."""
import runpy
from pathlib import Path

if __name__ == "__main__":
    runpy.run_path(str(Path(__file__).resolve().parent.parent / "scripts" / "keitaro_hub_campaign_bootstrap.py"))
