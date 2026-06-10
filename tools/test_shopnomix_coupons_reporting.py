#!/usr/bin/env python3
"""Smoke test: Shopnomix coupons placement click-level reporting API."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv

load_dotenv()

from config import (
    SHOPNOMIX_COUPONS_CAMPAIGN_ID,
    SHOPNOMIX_COUPONS_REPORTING_API_TOKEN,
    SHOPNOMIX_TILE_CAMPAIGN_ID,
    SHOPNOMIX_TILE_REPORTING_API_TOKEN,
    shopnomix_reporting_enabled,
)
from integrations.shopnomix import ShopnomixClientError, fetch_shopnomix_reporting_conversions


def main() -> int:
    ap = argparse.ArgumentParser(description="Test Shopnomix coupons reporting (v2 conversion).")
    ap.add_argument("--start", help="YYYY-MM-DD (default: 7 days ago)")
    ap.add_argument("--end", help="YYYY-MM-DD (default: yesterday UTC)")
    ap.add_argument("--limit", type=int, default=1000, help="Page size (max 50000)")
    ap.add_argument("--sample", type=int, default=5, help="Sample rows to print")
    args = ap.parse_args()

    if not shopnomix_reporting_enabled():
        print(
            "Missing config: set SHOPNOMIX_TILE/COUPONS_CAMPAIGN_ID and "
            "SHOPNOMIX_TILE/COUPONS_REPORTING_API_TOKEN env vars.",
            file=sys.stderr,
        )
        return 1

    end_d = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)
    start_d = date.fromisoformat(args.start) if args.start else end_d - timedelta(days=6)
    start_s, end_s = start_d.isoformat(), end_d.isoformat()

    print(f"Tile campaign:    {SHOPNOMIX_TILE_CAMPAIGN_ID}")
    print(f"Coupons campaign: {SHOPNOMIX_COUPONS_CAMPAIGN_ID}")
    print(f"Window:           {start_s} .. {end_s} (click time, inclusive)")

    try:
        rows, counts = fetch_shopnomix_reporting_conversions(start_s, end_s, limit=args.limit)
    except ShopnomixClientError as e:
        print(f"API error: {e}", file=sys.stderr)
        if e.response_body:
            print(e.response_body[:500], file=sys.stderr)
        return 1

    print(f"Rows:     {len(rows)} (tile={counts.get('tile', 0)}, coupons={counts.get('coupons', 0)})")
    if not rows:
        print("No conversions in range (API ok).")
        return 0

    by_status: dict[str, int] = {}
    total_rev = 0.0
    for row in rows:
        st = str(row.get("status") or "?").upper()
        by_status[st] = by_status.get(st, 0) + 1
        try:
            total_rev += float(str(row.get("revenue") or "0").replace(",", "."))
        except ValueError:
            pass

    print(f"Status:   {by_status}")
    print(f"Revenue:  ${total_rev:.4f} USD (sum of row revenue)")
    print()
    print("Sample click-level rows:")
    for row in rows[: max(0, args.sample)]:
        print(
            json.dumps(
                {
                    "click_id": row.get("click_id"),
                    "click_time": row.get("click_time"),
                    "country": row.get("country"),
                    "root_domain": row.get("root_domain"),
                    "revenue": row.get("revenue"),
                    "status": row.get("status"),
                    "source": row.get("source"),
                },
                ensure_ascii=False,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
