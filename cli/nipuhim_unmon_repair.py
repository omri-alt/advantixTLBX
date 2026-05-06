#!/usr/bin/env python3
"""
CLI: same run as AutoServer ``NipuhimUnmonRepair`` (manual trigger).
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from automations.autoserver.nipuhim_unmon_repair import NipuhimUnmonRepair  # noqa: E402


def main() -> None:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
    out = NipuhimUnmonRepair().run_manually()
    print(out)


if __name__ == "__main__":
    main()
