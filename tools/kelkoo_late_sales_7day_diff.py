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
from kelkoo_late_sales import prune_old_sales_workbook_tabs, run_late_sales_flow  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="Kelkoo late-sales 7-day diff (dry-run or apply).")
    ap.add_argument("--spreadsheet-id", default="", help="Override spreadsheet id.")
    ap.add_argument("--as-of", default="", help="Generation date YYYY-MM-DD of newer 7-day tab (default: latest).")
    ap.add_argument("--apply", action="store_true", help="Send LateSale GET postbacks and append monthly log.")
    ap.add_argument(
        "--prune-tabs",
        action="store_true",
        help="Delete SalesReport tabs older than KELKOO_SALES_TAB_RETENTION_DAYS (default 14).",
    )
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

    res = run_late_sales_flow(
        credentials_path=creds_path,
        spreadsheet_id=sid,
        postback_base=LATE_SALES_POSTBACK_BASE,
        as_of_str=args.as_of,
        apply=bool(args.apply),
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
    print(
        f"Candidates: total={res.get('candidate_count', res.get('new_count', 0))} "
        f"diff={res.get('diff_count', 0)} missed_on_sheet={res.get('missed_on_sheet', 0)} "
        f"raw_backfill={res.get('raw_backfill', 0)}"
    )
    print(f"Late-sale rows (eligible pipeline): {res.get('new_count', 0)}\n")

    print(
        f"Mode={res.get('mode')} skipped_keitaro={res.get('skipped_keitaro', 0)} "
        f"skipped_daily={res.get('skipped_daily', 0)} skipped_log={res.get('skipped_logged', 0)} "
        f"eligible={res.get('eligible_count', 0)} sent_ok={res.get('postbacks_ok')} sent_fail={res.get('postbacks_fail')} "
        f"log_tab={res.get('log_sheet')} log_rows={res.get('log_rows_appended')}"
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
