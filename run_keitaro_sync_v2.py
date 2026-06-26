#!/usr/bin/env python3
"""
Sync offers sheets into NIPUHIM-feed* hub child campaigns only (v2).

Does not touch legacy Nipuh (HrQBXp). Use after the daily workflow wrote offers tabs.

  python run_keitaro_sync_v2.py
  python run_keitaro_sync_v2.py --date 2026-06-25
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv

load_dotenv()

from run_daily_workflow import run_nipuhim_v2_keitaro_sync


def main() -> int:
    argv = sys.argv[1:]
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    feed1_only = False
    i = 0
    while i < len(argv):
        if argv[i] == "--date" and i + 1 < len(argv):
            date_str = argv[i + 1].strip()
            i += 2
            continue
        if argv[i] == "--feed1-traffic-only":
            feed1_only = True
            i += 1
            continue
        i += 1

    print(f"Nipuhim v2 Keitaro sync for {date_str}")
    print()
    ok = run_nipuhim_v2_keitaro_sync(date_str, feed1_traffic_only=feed1_only)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
