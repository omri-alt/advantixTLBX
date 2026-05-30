#!/usr/bin/env python3
"""
Effinity MTD backlog: ``CPAsale`` postbacks (payout=commissionAmount) for sales missing in Keitaro.

  python tools/run_effinity_mtd_cpasale_backlog.py --dry-run
  python tools/run_effinity_mtd_cpasale_backlog.py --apply
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from late_conversion_sales import apply_effinity_mtd_cpasale_backlog  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="Effinity MTD CPAsale backlog postbacks.")
    ap.add_argument("--dry-run", action="store_true", help="Count only; do not send GETs.")
    ap.add_argument("--apply", action="store_true", help="Send CPAsale GET postbacks.")
    args = ap.parse_args()
    if args.apply and args.dry_run:
        print("Use --dry-run or --apply, not both.")
        sys.exit(2)
    if not args.apply and not args.dry_run:
        args.dry_run = True

    res = apply_effinity_mtd_cpasale_backlog(dry_run=not args.apply)
    if res.get("error"):
        print("ERROR:", res["error"])
        sys.exit(1)
    print(
        f"mode={res.get('mode')} window={res.get('sale_window')} "
        f"effinity_sales={res.get('effinity_sales_found')} skipped_keitaro={res.get('skipped_keitaro')} "
        f"eligible={res.get('eligible')} sent_ok={res.get('postbacks_ok')} sent_fail={res.get('postbacks_fail')}"
    )
    for u in res.get("sample_urls") or []:
        print(" ", u)


if __name__ == "__main__":
    main()
