#!/usr/bin/env python3
"""
CLI: print new late-sale rows + postback URLs (dry-run only; use UI or code for apply).

Delegates to ``kelkoo_late_sales`` (same logic as ``/kelkoo/late-sales``).

  python tools/kelkoo_late_sales_7day_diff.py
  python tools/kelkoo_late_sales_7day_diff.py --spreadsheet-id <id>
  python tools/kelkoo_late_sales_7day_diff.py --as-of 2026-04-12
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from config import KELKOO_LATE_SALES_SPREADSHEET_ID, LATE_SALES_POSTBACK_BASE  # noqa: E402
from kelkoo_late_sales import run_late_sales_flow  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="Kelkoo late-sales 7-day diff (prints postback URLs; no HTTP).")
    ap.add_argument("--spreadsheet-id", default="", help="Override spreadsheet id.")
    ap.add_argument("--as-of", default="", help="Generation date YYYY-MM-DD of newer 7-day tab (default: latest).")
    args = ap.parse_args()

    sid = (args.spreadsheet_id or "").strip() or KELKOO_LATE_SALES_SPREADSHEET_ID
    creds_path = ROOT / "credentials.json"
    if not creds_path.exists():
        print("ERROR: credentials.json not found at", creds_path)
        sys.exit(1)

    res = run_late_sales_flow(
        credentials_path=creds_path,
        spreadsheet_id=sid,
        postback_base=LATE_SALES_POSTBACK_BASE,
        as_of_str=args.as_of,
        apply=False,
    )
    if res.get("error"):
        print("ERROR:", res["error"])
        sys.exit(1)

    utc_today = datetime.now(timezone.utc).date()
    print("UTC today (reference):", utc_today)
    print("Spreadsheet:", res.get("spreadsheet_title"), "| id:", res.get("spreadsheet_id"))
    print("NEWER:", res.get("tab_new"), "gen", res.get("d_new"), "| sale dates", res.get("window_new"))
    print("OLDER:", res.get("tab_old"), "gen", res.get("d_old"), "| sale dates", res.get("window_old"))
    print(
        f"Filtered: newer dropped={res.get('drop_new')} older dropped={res.get('drop_old')} dup={res.get('dup_new')}"
    )
    print(f"New late-sale rows: {res.get('new_count', 0)}\n")

    print(
        f"Skipped daily={res.get('skipped_daily', 0)} logged={res.get('skipped_logged', 0)} "
        f"eligible={res.get('eligible_count', 0)} log_tab={res.get('log_sheet')}"
    )
    print()
    for r in res.get("rows") or []:
        sr = r.get("skip_reason") or ""
        if sr:
            print(f"(skip {sr}) {r.get('postback_url')}")
        else:
            print(r.get("postback_url"))
        print(
            f"  click_id={r.get('click_id')} date={r.get('date')} merchant={r.get('merchant')} "
            f"sale_value_usd={r.get('sale_value_usd')} country={r.get('country')}"
        )
        print()


if __name__ == "__main__":
    main()
