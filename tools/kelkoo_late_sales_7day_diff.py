#!/usr/bin/env python3
"""
CLI: late conversion sales (MTD refresh + Keitaro check + optional LateSale apply).

  python tools/kelkoo_late_sales_7day_diff.py
  python tools/kelkoo_late_sales_7day_diff.py --dry-run
  python tools/kelkoo_late_sales_7day_diff.py --apply
  python tools/kelkoo_late_sales_7day_diff.py --prune-tabs
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from config import KELKOO_LATE_SALES_SPREADSHEET_ID, LATE_SALES_POSTBACK_BASE  # noqa: E402
from kelkoo_late_sales import prune_old_sales_workbook_tabs, run_late_sales_flow  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="Late conversion sales (MTD feeds vs Keitaro log).")
    ap.add_argument("--spreadsheet-id", default="", help="Override spreadsheet id.")
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Default: refresh MTD sheets, list LateSale URLs, do not send GETs (same as omitting --apply).",
    )
    ap.add_argument("--apply", action="store_true", help="Send LateSale GET postbacks.")
    ap.add_argument("--no-refresh", action="store_true", help="Skip MTD sheet refresh.")
    ap.add_argument("--prune-tabs", action="store_true", help="Delete legacy SalesReport / old MTD tabs.")
    ap.add_argument("--prune-dry-run", action="store_true", help="List tabs that would be pruned only.")
    args = ap.parse_args()

    sid = (args.spreadsheet_id or "").strip() or KELKOO_LATE_SALES_SPREADSHEET_ID
    creds_path = ROOT / "credentials.json"
    if not creds_path.exists():
        print("ERROR: credentials.json not found at", creds_path)
        sys.exit(1)

    if args.prune_tabs or args.prune_dry_run:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        creds = service_account.Credentials.from_service_account_file(str(creds_path))
        svc = build("sheets", "v4", credentials=creds).spreadsheets()
        removed = prune_old_sales_workbook_tabs(svc, sid, dry_run=bool(args.prune_dry_run))
        mode = "would delete" if args.prune_dry_run else "deleted"
        print(f"Tab prune: {mode} {len(removed)} tab(s)")
        for t in sorted(removed)[:40]:
            print(" ", t)
        if len(removed) > 40:
            print(f"  ... and {len(removed) - 40} more")
        if args.prune_dry_run or (args.prune_tabs and not args.apply):
            return

    if args.apply and args.dry_run:
        print("ERROR: use either --dry-run or --apply, not both.")
        sys.exit(2)

    mode = "apply" if args.apply else "dry-run"
    print(f"Late conversion ({mode}): starting ... (feeds + Keitaro log + sheets; often 3-15 min)", flush=True)

    res = run_late_sales_flow(
        credentials_path=creds_path,
        spreadsheet_id=sid,
        postback_base=LATE_SALES_POSTBACK_BASE,
        as_of_str="",
        apply=bool(args.apply) and not args.dry_run,
        refresh_sheets=not args.no_refresh,
        prune_tabs=not args.no_refresh,
    )
    if res.get("error"):
        print("ERROR:", res["error"])
        sys.exit(1)

    print("Spreadsheet:", res.get("spreadsheet_title"), "| id:", res.get("spreadsheet_id"))
    print("Month:", res.get("month_key"), "| sale window:", res.get("sale_window"))
    print(
        f"Mode={res.get('mode')} keitaro_keys={res.get('keitaro_keys_loaded')} "
        f"skipped_keitaro={res.get('skipped_keitaro')} eligible={res.get('eligible_count')} "
        f"sent_ok={res.get('postbacks_ok')} sent_fail={res.get('postbacks_fail')}"
    )
    print()
    for r in res.get("rows") or []:
        if r.get("skip_reason"):
            print(f"(skip {r.get('skip_reason')}) {r.get('postback_url')}")
        else:
            print(r.get("postback_url"))
        print(
            f"  feed={r.get('feed')} sub_id={r.get('click_id')} date={r.get('date')} "
            f"payout={r.get('sale_value_usd')} merchant={r.get('merchant')}"
        )
        print()


if __name__ == "__main__":
    main()
