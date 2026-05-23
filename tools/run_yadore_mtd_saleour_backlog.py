#!/usr/bin/env python3
"""
Yadore MTD backlog: ``SaleOur`` postbacks (payout=0) for conversion/detail sales missing in Keitaro.

  python tools/run_yadore_mtd_saleour_backlog.py --dry-run
  python tools/run_yadore_mtd_saleour_backlog.py --apply
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from late_conversion_sales import apply_yadore_saleour_backlog  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="Yadore MTD SaleOur backlog postbacks.")
    ap.add_argument("--dry-run", action="store_true", help="Count only; do not send GETs.")
    ap.add_argument("--apply", action="store_true", help="Send SaleOur GET postbacks.")
    args = ap.parse_args()
    if args.apply and args.dry_run:
        print("Use --dry-run or --apply, not both.")
        sys.exit(2)
    if not args.apply and not args.dry_run:
        args.dry_run = True

    res = apply_yadore_saleour_backlog(dry_run=not args.apply)
    if res.get("error"):
        print("ERROR:", res["error"])
        sys.exit(1)
    print(
        f"mode={res.get('mode')} window={res.get('sale_window')} "
        f"yadore_sales={res.get('yadore_sales_found')} skipped_keitaro={res.get('skipped_keitaro')} "
        f"eligible={res.get('eligible')} sent_ok={res.get('postbacks_ok')} sent_fail={res.get('postbacks_fail')}"
    )
    for u in res.get("sample_urls") or []:
        print(" ", u)


if __name__ == "__main__":
    main()
