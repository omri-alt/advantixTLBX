#!/usr/bin/env python3
"""
Send daily Keitaro conversion postbacks from Kelkoo (per geo), Adexa StatsRaw, and Yadore report/detail.

State file (resume, no double-fire): ``DAILY_CONVERSION_POSTBACK_STATE_PATH`` (see ``config.py``).

  python run_daily_conversion_postbacks.py --dry-run
  python run_daily_conversion_postbacks.py --report-date 2026-04-08
  python run_daily_conversion_postbacks.py --only kelkoo1 --geo uk
  python run_daily_conversion_postbacks.py --reset kelkoo1 --report-date 2026-04-08
  python run_daily_conversion_postbacks.py --no-resume --only adexa

CLI mirror: ``python cli/run_daily_conversion_postbacks.py`` (same args).
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

    ap = argparse.ArgumentParser(description="Daily Keitaro conversion postbacks (Kelkoo per geo, Adexa, Yadore).")
    ap.add_argument(
        "--report-date",
        default=default_report_date_str(),
        help="Stats date YYYY-MM-DD (default: yesterday UTC).",
    )
    ap.add_argument(
        "--only",
        default="all",
        help="kelkoo1 | kelkoo2 | adexa | yadore | all",
    )
    ap.add_argument("--geo", default="", help="Kelkoo only: process a single country code (e.g. uk).")
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not call postback endpoints or write resume state. Logs example URL shapes for Kelkoo; Adexa/Yadore log each postback URL at INFO.",
    )
    ap.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore saved progress for this run (still updates state after sends — use --reset to clear).",
    )
    ap.add_argument(
        "--reset",
        action="append",
        default=[],
        metavar="SOURCE",
        help="Clear saved state for SOURCE@report-date before running (repeatable). kelkoo1, kelkoo2, adexa, yadore.",
    )
    args = ap.parse_args()

    rc = run_daily_conversion_postbacks_main(
        report_date=args.report_date.strip(),
        only=args.only,
        only_geo=args.geo.strip() or None,
        dry_run=bool(args.dry_run),
        no_resume=bool(args.no_resume),
        reset_sources=args.reset or None,
    )
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
