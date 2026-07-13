#!/usr/bin/env python3
"""
Sync domain-demand bill (hub campaign 94) to Google Sheets.

Tabs on ``DOMAIN_DEMAND_SHEET_ID``:
  - ``summary`` — family/feed rollups, hub weights, Trillion pause hint
  - ``bill`` — per-line demand vs delivered (Nipuhim feed×geo + Blend rows)

Usage:
  python scripts/domain_demand_sync.py
  python scripts/domain_demand_sync.py --dry-run
  python scripts/domain_demand_sync.py --date 2026-07-09
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from config import DOMAIN_DEMAND_ENABLED  # noqa: E402
from integrations.domain_demand import sync_domain_demand  # noqa: E402

_DEFAULT_MAX_OFFERS = 60


def main() -> int:
    p = argparse.ArgumentParser(description="Sync domain-demand summary + bill sheets.")
    p.add_argument("--dry-run", action="store_true", help="Build payload only; do not write Sheets.")
    p.add_argument("--date", metavar="YYYY-MM-DD", help="Calendar day (default: report TZ today).")
    p.add_argument(
        "--max-offers-per-geo",
        type=int,
        default=_DEFAULT_MAX_OFFERS,
        help="Nipuhim offers tab cap per geo when scanning demand.",
    )
    p.add_argument(
        "--delivered-only",
        action="store_true",
        help="Skip rebuilding demand lines (refresh Keitaro delivered only; needs prior bill).",
    )
    p.add_argument("--json", action="store_true", help="Print full JSON payload to stdout.")
    args = p.parse_args()

    if not DOMAIN_DEMAND_ENABLED:
        print("DOMAIN_DEMAND_ENABLED=0 — skipped.")
        return 0

    result = sync_domain_demand(
        date_str=args.date,
        max_offers_per_geo=args.max_offers_per_geo,
        rebuild_demand=not args.delivered_only,
        dry_run=args.dry_run,
        reason="cli",
    )

    for line in result.get("logs") or []:
        print(line)

    write = result.get("write") or {}
    print(
        f"Domain demand: {write.get('status')} — "
        f"bill={write.get('bill_rows', 0)} summary={write.get('summary_rows', 0)} "
        f"by_geo={write.get('geo_rows', 0)} "
        f"total_demand={result.get('total_demand')} "
        f"delivered={result.get('total_delivered_child_sum')} "
        f"trillion={result.get('trillion_hint')}"
    )

    if args.json:
        slim = {k: v for k, v in result.items() if k not in ("bill", "summary")}
        print(json.dumps(slim, indent=2, ensure_ascii=False))

    return 0 if write.get("status") in ("ok", "dry_run") else 1


if __name__ == "__main__":
    raise SystemExit(main())
