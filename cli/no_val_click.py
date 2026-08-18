#!/usr/bin/env python3
"""Preview yesterday's Nipuhim/Blend offers with clicks and 0 Val_clicks."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Find yesterday offers that received clicks but no Val_clicks. "
            "Daily workflow skips those Nipuhim merchants and colors Blend rows."
        )
    )
    parser.add_argument("--date", default="", help="Run date YYYY-MM-DD (analysis uses the previous UTC day).")
    parser.add_argument("--min-clicks", type=int, default=0, help="Override NO_VAL_CLICK_MIN_CLICKS.")
    parser.add_argument(
        "--apply-blend-colors",
        action="store_true",
        help="Write light-red / white backgrounds on the Blend sheet.",
    )
    parser.add_argument("--scope", choices=("all", "nipuhim", "blend"), default="all")
    args = parser.parse_args()
    try:
        from dotenv import load_dotenv
    except ImportError:
        load_dotenv = None  # type: ignore[assignment]
    if load_dotenv:
        load_dotenv(ROOT / ".env")

    from datetime import datetime

    from integrations.no_val_click import analyze_yesterday, color_blend_dead_rows, yesterday_utc

    run_date = None
    if (args.date or "").strip():
        run_date = datetime.strptime(args.date.strip(), "%Y-%m-%d").date()
    min_clicks = int(args.min_clicks) if args.min_clicks and args.min_clicks > 0 else None
    payload = analyze_yesterday(
        run_date=run_date,
        min_clicks=min_clicks,
        persist=True,
        scope=args.scope,
    )
    public = {k: v for k, v in payload.items() if not str(k).startswith("_")}
    public["yesterday"] = yesterday_utc(run_date).isoformat()
    print(json.dumps(public, indent=2, default=str))
    if args.apply_blend_colors:
        res = color_blend_dead_rows(run_date=run_date, dry_run=False, min_clicks=min_clicks)
        print(json.dumps({"blend_colors": res}, indent=2, default=str))
        if res.get("status") == "error":
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
