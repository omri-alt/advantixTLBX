#!/usr/bin/env python3
"""
Yadore MTD backlog: ``SaleOur`` postbacks (payout=0) for conversion/detail sales missing in Keitaro.

  python tools/run_yadore_mtd_saleour_backlog.py --dry-run
  python tools/run_yadore_mtd_saleour_backlog.py --apply
  python tools/run_yadore_mtd_saleour_backlog.py --from 2026-06-01 --to 2026-06-30 --apply
  python tools/run_yadore_mtd_saleour_backlog.py --diff-json data/yadore_keitaro_diff_2026-06.json --apply
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from late_conversion_sales import (  # noqa: E402
    apply_yadore_saleour_backlog,
    apply_yadore_saleour_missing_rows,
)


def _parse_date(s: str) -> date:
    return date.fromisoformat(s.strip()[:10])


def main() -> None:
    ap = argparse.ArgumentParser(description="Yadore MTD SaleOur backlog postbacks.")
    ap.add_argument("--dry-run", action="store_true", help="Count only; do not send GETs.")
    ap.add_argument("--apply", action="store_true", help="Send SaleOur GET postbacks.")
    ap.add_argument("--from", dest="date_from", metavar="YYYY-MM-DD", help="Sale window start.")
    ap.add_argument("--to", dest="date_to", metavar="YYYY-MM-DD", help="Sale window end (inclusive).")
    ap.add_argument(
        "--diff-json",
        metavar="PATH",
        help="Send from missing_in_keitaro_rows in a yadore_sales_keitaro_diff JSON report.",
    )
    args = ap.parse_args()
    if args.apply and args.dry_run:
        print("Use --dry-run or --apply, not both.")
        sys.exit(2)
    if not args.apply and not args.dry_run:
        args.dry_run = True

    dry_run = not args.apply

    if args.diff_json:
        path = Path(args.diff_json)
        if not path.is_file():
            print(f"ERROR: file not found: {path}")
            sys.exit(1)
        report = json.loads(path.read_text(encoding="utf-8"))
        missing = report.get("missing_in_keitaro_rows") or []
        keitaro_from = None
        if args.date_from:
            keitaro_from = _parse_date(args.date_from)
        elif report.get("date_from"):
            keitaro_from = _parse_date(str(report["date_from"]))
        res = apply_yadore_saleour_missing_rows(
            missing,
            dry_run=dry_run,
            keitaro_from=keitaro_from,
        )
    else:
        start = _parse_date(args.date_from) if args.date_from else None
        end = _parse_date(args.date_to) if args.date_to else None
        res = apply_yadore_saleour_backlog(dry_run=dry_run, start_date=start, end_date=end)

    if res.get("error"):
        print("ERROR:", res["error"])
        sys.exit(1)
    print(
        f"mode={res.get('mode')} window={res.get('sale_window') or res.get('keitaro_dedup_from')} "
        f"input={res.get('yadore_sales_found') or res.get('input_rows')} "
        f"skipped_keitaro={res.get('skipped_keitaro')} "
        f"eligible={res.get('eligible')} sent_ok={res.get('postbacks_ok')} sent_fail={res.get('postbacks_fail')}"
    )
    for u in res.get("sample_urls") or []:
        print(" ", u)
    for f in res.get("failures") or []:
        print(" FAIL:", f)
    if not res.get("ok"):
        sys.exit(1)


if __name__ == "__main__":
    main()
