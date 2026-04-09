#!/usr/bin/env python3
"""
Run the Blend workflow:
  - Sync Keitaro campaign 'Blend' (alias 9Xq9dSMh) from the 'Blend' sheet.
  - Generate `potentialKelkoo1` / `potentialKelkoo2` from Kelkoo reports (requires --feed per run).

Usage:
  python run_blend_workflow.py
  python run_blend_workflow.py --feed kelkoo1
  python run_blend_workflow.py --feed kelkoo2
  python run_blend_workflow.py --feed both
  python run_blend_workflow.py --geo fr
  python run_blend_workflow.py --skip-potential
  python run_blend_workflow.py --only-potential
  python run_blend_workflow.py --start 2026-03-01 --end 2026-03-10
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def main() -> None:
    p = argparse.ArgumentParser(description="Blend: Keitaro sync + potentialKelkoo sheets")
    p.add_argument(
        "--feed",
        default="both",
        choices=["kelkoo1", "kelkoo2", "both"],
        help="Kelkoo feed for potentialBlends generation (default: both = kelkoo1 then kelkoo2).",
    )
    p.add_argument("--geo", default=None, help="Passed to blend_sync_from_sheet.")
    p.add_argument("--skip-potential", action="store_true", help="Skip blend_potential_merchants.")
    p.add_argument("--only-potential", action="store_true", help="Only run blend_potential_merchants (no Keitaro sync).")
    p.add_argument("--start", default=None, help="Report range start (blend_potential_merchants).")
    p.add_argument("--end", default=None, help="Report range end (blend_potential_merchants).")
    p.add_argument(
        "--only-monetized",
        action="store_true",
        help="Forwarded to blend_potential_merchants (hide unmonetized rows).",
    )
    args = p.parse_args()

    feeds = ["kelkoo1", "kelkoo2"] if args.feed == "both" else [args.feed]

    root = Path(__file__).resolve().parent
    sync_script = root / "blend_sync_from_sheet.py"
    potential_script = root / "blend_potential_merchants.py"

    if not args.only_potential:
        cmd = [sys.executable, str(sync_script)]
        if args.geo:
            cmd += ["--geo", args.geo]
        print("1) Syncing Blend offers/flows to Keitaro ...")
        if subprocess.run(cmd).returncode != 0:
            sys.exit(1)
        print()

    if args.skip_potential:
        print("Skipping potentialBlends generation (--skip-potential).")
        return

    for feed in feeds:
        cmd = [sys.executable, str(potential_script), "--feed", feed]
        if args.start:
            cmd += ["--start", args.start]
        if args.end:
            cmd += ["--end", args.end]
        if args.only_monetized:
            cmd += ["--only-monetized"]
        print(f"2) Generating potentialBlends from Kelkoo reports ({feed}) ...")
        if subprocess.run(cmd).returncode != 0:
            sys.exit(1)


if __name__ == "__main__":
    main()
