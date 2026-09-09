#!/usr/bin/env python3
"""Fan child Val_click conversions to Keitaro hub campaign 94 (payout 0)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from integrations.hub_val_click_postbacks import cli_main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(cli_main())
