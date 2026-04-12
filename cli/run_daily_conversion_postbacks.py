#!/usr/bin/env python3
"""Wrapper: runs ``run_daily_conversion_postbacks`` from repo root (stable path)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from run_daily_conversion_postbacks import main  # noqa: E402


if __name__ == "__main__":
    main()
