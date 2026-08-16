#!/usr/bin/env python3
"""
Scheduled Kelkoo daily conversion postbacks (probe → ready geos → hourly retry).

Cron-friendly entry (no Flask required):

  python cli/run_kelkoo_daily_postbacks_scheduled.py
  python cli/run_kelkoo_daily_postbacks_scheduled.py --dry-run
  python cli/run_kelkoo_daily_postbacks_scheduled.py --date 2026-08-03

In-process schedule (when Control Center / Gunicorn runs): APScheduler cron
``kelkoo_daily_conversion_postbacks`` hourly from
``KELKOO_DAILY_POSTBACK_SCHEDULER_HOUR_LOCAL`` through 23:00
(default 08:00 Asia/Jerusalem first try). Missing geos retry each hour.
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

from scheduler.kelkoo_daily_postbacks_scheduler import (  # noqa: E402
    run_kelkoo_daily_postbacks_scheduled,
)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Kelkoo daily postbacks: probe geos, process ready, retry missing hourly."
    )
    ap.add_argument("--date", default="", help="Report date YYYY-MM-DD (default: yesterday UTC).")
    ap.add_argument("--dry-run", action="store_true", help="Probe + process without GET postbacks / retries.")
    ap.add_argument(
        "--pending-only",
        action="store_true",
        help="Only retry geos listed in runtime/kelkoo_daily_postbacks_pending.json.",
    )
    ap.add_argument("--attempt", type=int, default=1, help="Attempt number (for logging / max cap).")
    ap.add_argument("--json", action="store_true", help="Print full JSON result.")
    args = ap.parse_args()

    out = run_kelkoo_daily_postbacks_scheduled(
        report_date=(args.date or None),
        attempt=int(args.attempt),
        pending_only=bool(args.pending_only),
        dry_run=bool(args.dry_run),
        triggered_by="cli",
    )
    if args.json:
        print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    else:
        pending = out.get("pending_by_feed") or {}
        print(
            f"ok={out.get('ok')} date={out.get('report_date')} attempt={out.get('attempt')} "
            f"pending_feeds={list(pending.keys())} retry={bool(out.get('retry_scheduled'))}"
        )
        for feed, summ in (out.get("feeds") or {}).items():
            if not isinstance(summ, dict):
                continue
            print(
                f"  {feed}: ready={len(summ.get('ready') or [])} "
                f"not_ready={len(summ.get('not_ready') or [])} "
                f"already_done={len(summ.get('already_done') or [])}"
            )
        if out.get("error"):
            print("ERROR:", out["error"])
    if not out.get("ok"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
