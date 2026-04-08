#!/usr/bin/env python3
"""
Run only the Keitaro sync step: read offers from the sheet and push to Keitaro.

Use this when the daily workflow already wrote the offers sheets (steps 1–5)
but the Keitaro upload failed or was skipped. Reads YYYY-MM-DD_offers_1 and
YYYY-MM-DD_offers_2 and runs update_offers_from_sheet for each.

  python run_keitaro_sync.py
  python run_keitaro_sync.py --date 2026-03-11
"""
import sys
import subprocess
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv()

def main():
    argv = sys.argv[1:]
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    i = 0
    while i < len(argv):
        if argv[i] == "--date" and i + 1 < len(argv):
            date_str = argv[i + 1].strip()
            i += 2
            continue
        i += 1

    sheet1 = f"{date_str}_offers_1"
    sheet2 = f"{date_str}_offers_2"
    script = Path(__file__).resolve().parent / "update_offers_from_sheet.py"

    print(f"Keitaro sync only for date {date_str}")
    print(f"  Feed1 sheet: {sheet1}")
    print(f"  Feed2 sheet: {sheet2}")
    print()

    r1 = subprocess.run([sys.executable, str(script), "--sheet", sheet1])
    if r1.returncode != 0:
        print("Feed1 sync failed.")
        sys.exit(1)
    print()
    r2 = subprocess.run([sys.executable, str(script), "--sheet", sheet2, "--account", "2"])
    if r2.returncode != 0:
        print("Feed2 sync failed.")
        sys.exit(1)
    print()
    print("Done. Both feeds synced to Keitaro.")


if __name__ == "__main__":
    main()
