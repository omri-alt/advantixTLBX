#!/usr/bin/env python3
"""
Run the Blend workflow:
  - Sync Keitaro campaign 'Blend' (alias 9Xq9dSMh) from the 'Blend' sheet.
  - Generate `potentialBlends` from Kelkoo reports (feed1) for merchants listed in the sheet.

Usage:
  python run_blend_workflow.py
  python run_blend_workflow.py --geo fr
  python run_blend_workflow.py --skip-potential
  python run_blend_workflow.py --only-potential
  python run_blend_workflow.py --start 2026-03-01 --end 2026-03-10
"""
import subprocess
import sys
from pathlib import Path


def main() -> None:
    argv = sys.argv[1:]
    skip_potential = "--skip-potential" in argv
    only_potential = "--only-potential" in argv

    # passthrough args
    geo = None
    start = None
    end = None
    i = 0
    while i < len(argv):
        if argv[i] == "--geo" and i + 1 < len(argv):
            geo = argv[i + 1]
            i += 2
            continue
        if argv[i] == "--start" and i + 1 < len(argv):
            start = argv[i + 1]
            i += 2
            continue
        if argv[i] == "--end" and i + 1 < len(argv):
            end = argv[i + 1]
            i += 2
            continue
        i += 1

    root = Path(__file__).resolve().parent
    sync_script = root / "blend_sync_from_sheet.py"
    potential_script = root / "blend_potential_merchants.py"

    if not only_potential:
        cmd = [sys.executable, str(sync_script)]
        if geo:
            cmd += ["--geo", geo]
        print("1) Syncing Blend offers/flows to Keitaro ...")
        if subprocess.run(cmd).returncode != 0:
            sys.exit(1)
        print()

    if skip_potential:
        print("Skipping potentialBlends generation (--skip-potential).")
        return

    cmd = [sys.executable, str(potential_script)]
    if start:
        cmd += ["--start", start]
    if end:
        cmd += ["--end", end]
    print("2) Generating potentialBlends from Kelkoo reports ...")
    if subprocess.run(cmd).returncode != 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

