#!/usr/bin/env python3
"""
Shopnomix feed6 daily conversion postbacks (tile + coupons placements in one run).

Thin wrapper around ``run_daily_conversion_postbacks_main(only='shopnomix')``.

  python run_shopnomix_conversion_postbacks.py --dry-run
  python run_shopnomix_conversion_postbacks.py --report-date 2026-06-09
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv

load_dotenv()

from integrations.daily_conversion_postbacks import (  # noqa: E402
    default_report_date_str,
    run_daily_conversion_postbacks_main,
)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    ap = argparse.ArgumentParser(
        description="Shopnomix feed6 postbacks (tile + coupons → Keitaro GET postbacks)."
    )
    ap.add_argument(
        "--report-date",
        default=default_report_date_str(),
        help="Stats date YYYY-MM-DD (default: yesterday UTC).",
    )
    ap.add_argument("--dry-run", action="store_true", help="Log URLs only; do not send or write state.")
    ap.add_argument("--no-resume", action="store_true", help="Ignore saved progress for this run.")
    ap.add_argument(
        "--reset",
        action="store_true",
        help="Clear shopnomix resume state for this report date before running.",
    )
    args = ap.parse_args()

    rc = run_daily_conversion_postbacks_main(
        report_date=args.report_date.strip(),
        only="shopnomix",
        only_geo=None,
        dry_run=bool(args.dry_run),
        no_resume=bool(args.no_resume),
        reset_sources=["shopnomix"] if args.reset else None,
    )
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
