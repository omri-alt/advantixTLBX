#!/usr/bin/env python3
"""Dry-run checkmon audit for Keitaro feed-balance campaigns (no share/notes writes)."""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from integrations.keitaro_feed_balance import (
    format_checkmon_report_block,
    run_checkmon_audit_dry,
    write_checkmon_audit_csv,
)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Checkmon dry-run for Keitaro campaigns with url+geo in notes (no weight changes)."
    )
    ap.add_argument("--campaign-id", type=int, action="append", dest="campaign_ids")
    ap.add_argument(
        "--output",
        type=Path,
        help="CSV path (default: runtime/keitaro_feed_balance_checkmon_<ts>.csv)",
    )
    ap.add_argument("--include-incomplete", action="store_true", help="Also run campaigns missing url/geo")
    args = ap.parse_args()

    result = run_checkmon_audit_dry(
        campaign_ids=args.campaign_ids,
        require_url_geo=not args.include_incomplete,
    )
    ts = result.get("timestamp_utc") or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out = args.output or (ROOT / "runtime" / f"keitaro_feed_balance_checkmon_{ts.replace(':', '').replace('-', '')[:15]}.csv")
    write_checkmon_audit_csv(result, out)

    print(f"Checkmon dry-run @ {ts}")
    print(f"Campaigns checked: {result.get('checked_campaigns', 0)}")
    print(f"Skipped (missing url/geo in notes): {result.get('skipped_incomplete_config', 0)}")
    print(f"CSV: {out}")
    print()

    for row in result.get("campaigns") or []:
        print(format_checkmon_report_block(row, ts=ts))
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
