#!/usr/bin/env python3
"""
Scheduled daily workflow v2 launcher (cron-friendly; no Flask required).

  python cli/run_daily_workflow_scheduled.py
  python cli/run_daily_workflow_scheduled.py --force
  python cli/run_daily_workflow_scheduled.py --extra-args "--skip-late-sales"

In-process schedule (when Control Center / Gunicorn runs): APScheduler cron
``daily_workflow_v2`` at ``DAILY_WORKFLOW_SCHEDULER_*`` (default 09:00 Asia/Jerusalem).
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

from scheduler.daily_workflow_scheduler import run_daily_workflow_scheduled  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="Launch daily workflow v2 (scheduled entry).")
    ap.add_argument(
        "--force",
        action="store_true",
        help="Start even if a successful run already finished today.",
    )
    ap.add_argument(
        "--extra-args",
        default="",
        help="CLI flags for run_daily_workflow_v2.py (overrides DAILY_WORKFLOW_SCHEDULER_EXTRA_ARGS).",
    )
    ns = ap.parse_args()
    extra = ns.extra_args if ns.extra_args.strip() else None
    out = run_daily_workflow_scheduled(
        triggered_by="cli",
        extra_args=extra,
        force=bool(ns.force),
    )
    print(json.dumps(out, ensure_ascii=True, indent=2))
    if not out.get("ok"):
        # already_running / already_succeeded_today are soft skips
        err = str(out.get("error") or "")
        if err in ("already_running", "already_succeeded_today", "another_launch_in_progress"):
            raise SystemExit(0)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
