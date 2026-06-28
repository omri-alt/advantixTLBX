#!/usr/bin/env python3
"""
Raise SK campaign bids from ``BulkBidRaising`` sheet and cap bid factors on boosted sources.

Tab on ``SK_TOOLS_SPREADSHEET_ID`` (default): ``BulkBidRaising``
  - campaignId
  - newCampaignBid
  - maxBidPerSource

Examples:
  python sk_bulk_bid_raising.py --setup-sheet
  python sk_bulk_bid_raising.py --dry-run
  python sk_bulk_bid_raising.py --apply
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv

load_dotenv()


def main() -> int:
    argv = sys.argv[1:]
    dry_run = True
    setup_only = False
    tab = "BulkBidRaising"

    i = 0
    while i < len(argv):
        a = argv[i]
        if a in ("--apply",):
            dry_run = False
            i += 1
            continue
        if a in ("--dry-run",):
            dry_run = True
            i += 1
            continue
        if a == "--setup-sheet":
            setup_only = True
            i += 1
            continue
        if a == "--tab" and i + 1 < len(argv):
            tab = argv[i + 1].strip()
            i += 2
            continue
        print(f"Unknown arg: {a}")
        return 2

    from integrations.sk_bulk_bid_raising import (
        ensure_bulk_bid_raising_sheet,
        run_bulk_bid_raising,
    )

    if setup_only:
        sid = ensure_bulk_bid_raising_sheet()
        print(f"BulkBidRaising tab ready on spreadsheet {sid}")
        return 0

    try:
        _results, log, all_ok = run_bulk_bid_raising(tab_name=tab, dry_run=dry_run)
    except Exception as e:
        print(f"Error: {e}")
        return 1
    print(log)
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
