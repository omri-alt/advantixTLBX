#!/usr/bin/env python3
"""
Daily Effinity conversion sync: API → Keitaro dedup → fire missing salecpa postbacks.

Cron-friendly entry (no Flask required):

  python cli/run_effinity_daily_postbacks.py
  python cli/run_effinity_daily_postbacks.py --dry-run

In-process schedule (when Control Center / Gunicorn runs): APScheduler cron
``effinity_mtd_salecpa_daily`` at ``EFFINITY_SALES_SCHEDULER_*`` (default 10:15 Asia/Jerusalem).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from scheduler.effinity_sales_scheduler import run_effinity_daily_postbacks  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="Effinity MTD sales → Keitaro salecpa postbacks.")
    ap.add_argument("--dry-run", action="store_true", help="Count missing conversions only.")
    args = ap.parse_args()

    out = run_effinity_daily_postbacks(dry_run=bool(args.dry_run))
    if out.get("error"):
        print("ERROR:", out["error"])
        raise SystemExit(1)
    print(
        f"mode={out.get('mode')} window={out.get('sale_window')} "
        f"effinity_sales={out.get('effinity_sales_found')} skipped_keitaro={out.get('skipped_keitaro')} "
        f"eligible={out.get('eligible')} sent_ok={out.get('postbacks_ok')} sent_fail={out.get('postbacks_fail')}"
    )
    for u in out.get("sample_urls") or []:
        print(" ", u)
    if not out.get("ok"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
