#!/usr/bin/env python3
"""
Scheduled Adexa + Yadore daily conversion postbacks (clicks).

Cron-friendly entry (no Flask required):

  python cli/run_adexa_yadore_daily_postbacks_scheduled.py
  python cli/run_adexa_yadore_daily_postbacks_scheduled.py --dry-run
  python cli/run_adexa_yadore_daily_postbacks_scheduled.py --date 2026-08-03

In-process schedule: APScheduler cron ``adexa_yadore_daily_conversion_postbacks``
at ``ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_*`` (default 12:30 Asia/Jerusalem).
Yadore sales remain on ``YADORE_SALES_SCHEDULER_*`` (default 10:00).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from scheduler.adexa_yadore_daily_postbacks_scheduler import (  # noqa: E402
    run_adexa_yadore_daily_postbacks_scheduled,
)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Adexa + Yadore daily click postbacks (skip if already ran today)."
    )
    ap.add_argument("--date", default="", help="Report date YYYY-MM-DD (default: yesterday UTC).")
    ap.add_argument("--dry-run", action="store_true", help="Fetch/process without GET postbacks.")
    ap.add_argument("--json", action="store_true", help="Print full JSON result.")
    args = ap.parse_args()

    out = run_adexa_yadore_daily_postbacks_scheduled(
        report_date=(args.date or None),
        dry_run=bool(args.dry_run),
        triggered_by="cli",
    )
    if args.json:
        print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    else:
        print(
            f"ok={out.get('ok')} date={out.get('report_date')} "
            f"skipped={out.get('skipped_already_ran_today')}"
        )
        for feed, summ in (out.get("feeds") or {}).items():
            if isinstance(summ, dict):
                print(f"  {feed}: ok={summ.get('ok')} skipped={bool(summ.get('skipped_already_ran_today'))}")
        if out.get("error"):
            print("ERROR:", out["error"])
    if not out.get("ok"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
