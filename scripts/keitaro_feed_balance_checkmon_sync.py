#!/usr/bin/env python3
"""Run feed-balance checkmon and update Keitaro campaign notes (no share changes)."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from integrations.keitaro_feed_balance import format_checkmon_report_block, run_checkmon_update_notes


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Checkmon sync for Keitaro feed-balance campaigns (updates notes only)."
    )
    ap.add_argument("--campaign-id", type=int, action="append", dest="campaign_ids")
    ap.add_argument("--dry-run", action="store_true", help="Check APIs but do not PUT notes")
    ap.add_argument(
        "--include-incomplete",
        action="store_true",
        help="Also run campaigns missing url/geo in notes",
    )
    args = ap.parse_args()

    result = run_checkmon_update_notes(
        campaign_ids=args.campaign_ids,
        dry_run=args.dry_run,
        require_url_geo=not args.include_incomplete,
    )
    ts = result.get("timestamp_utc") or ""
    mode = "dry-run" if args.dry_run else "apply"

    print(f"Feed balance checkmon sync ({mode}) @ {ts}")
    print(f"Campaigns checked: {result.get('checked_campaigns', 0)}")
    print(f"Notes updated: {result.get('notes_updated', 0)}")
    print(f"Skipped (unchanged): {result.get('notes_skipped_unchanged', 0)}")
    print(f"Skipped (no config): {result.get('notes_skipped_no_config', 0)}")
    print(f"Errors: {result.get('notes_errors', 0)}")
    print()

    for row in result.get("campaigns") or []:
        print(format_checkmon_report_block(row, ts=ts, dry_run=args.dry_run))
        print()

    return 1 if int(result.get("notes_errors") or 0) > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
