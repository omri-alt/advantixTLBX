#!/usr/bin/env python3
"""CLI wrapper for staged daily workflow v2."""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from run_daily_workflow_v2 import main

if __name__ == "__main__":
    raise SystemExit(main())
