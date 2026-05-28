#!/usr/bin/env python3
"""Run one daily workflow v2 stage (invoked by the orchestrator)."""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from workflows.daily_v2.run_stage import main

if __name__ == "__main__":
    raise SystemExit(main())
