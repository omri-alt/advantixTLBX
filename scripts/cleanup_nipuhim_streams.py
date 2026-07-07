#!/usr/bin/env python3
"""
Remove legacy HrQBXp country flows from NIPUHIM-feed* campaigns and move fallback last.

  python scripts/cleanup_nipuhim_streams.py           # dry-run
  python scripts/cleanup_nipuhim_streams.py --apply
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from config import KEITARO_API_KEY, KEITARO_BASE_URL
from integrations.nipuhim_stream_cleanup import cleanup_nipuhim_feed_campaigns


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean NIPUHIM-feed* Keitaro stream hierarchy.")
    parser.add_argument("--apply", action="store_true", help="Apply deletions and reordering.")
    args = parser.parse_args()

    if not (KEITARO_BASE_URL and KEITARO_API_KEY):
        print("Error: set KEITARO_BASE_URL and KEITARO_API_KEY in .env")
        return 1

    dry_run = not args.apply
    print(f"NIPUHIM stream cleanup — {'DRY-RUN' if dry_run else 'APPLY'}")
    print()

    result = cleanup_nipuhim_feed_campaigns(dry_run=dry_run)
    for line in result.get("logs") or []:
        print(line)

    print()
    totals = result.get("totals") or {}
    print(
        f"Totals: {totals.get('legacy_deleted', 0)} legacy flow(s), "
        f"{totals.get('positions_updated', 0)} position update(s)"
    )
    if dry_run:
        print("Dry-run complete. Re-run with --apply to push to Keitaro.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
