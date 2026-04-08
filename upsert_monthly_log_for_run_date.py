#!/usr/bin/env python3
"""
Upsert merchants from ``{run_date}_offers_1`` / ``_offers_2`` into ``{month}_log_1`` / ``_log_2``.

Default behavior matches the daily workflow addition:
  - always upsert merchants + names
  - do NOT run Kelkoo monetization checks unless ``--check-monetization`` is provided

Usage:
  python upsert_monthly_log_for_run_date.py --run-date 2026-03-23
  python upsert_monthly_log_for_run_date.py --run-date 2026-03-23 --feed 1
  python upsert_monthly_log_for_run_date.py --run-date 2026-03-23 --check-monetization
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv

load_dotenv()

from config import FEED1_API_KEY, FEED2_API_KEY, KELKOO_SHEETS_SPREADSHEET_ID
from workflows.monthly_log_monetization import upsert_run_merchants_into_monthly_log


def get_credentials_path() -> str:
    p = Path(__file__).resolve().parent / "credentials.json"
    if not p.exists():
        raise FileNotFoundError(f"credentials.json not found at {p}")
    return str(p)


def get_sheets_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_file(get_credentials_path())
    return build("sheets", "v4", credentials=creds).spreadsheets()


def main() -> None:
    parser = argparse.ArgumentParser(description="Upsert monthly log for a given run date.")
    parser.add_argument("--run-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--feed", choices=["1", "2", "both"], default="both")
    parser.add_argument("--check-monetization", action="store_true", help="Also fill column E (Kelkoo monetization).")
    args = parser.parse_args()

    try:
        datetime.strptime(args.run_date, "%Y-%m-%d")
    except ValueError:
        print("Error: --run-date must be YYYY-MM-DD", file=sys.stderr)
        sys.exit(1)

    feeds = [1, 2] if args.feed == "both" else [int(args.feed)]
    service = get_sheets_service()

    print(f"Spreadsheet: {KELKOO_SHEETS_SPREADSHEET_ID}")
    print(f"Run date: {args.run_date}, feeds: {feeds}, check_monetization={args.check_monetization}")
    print()

    calls_total = 0
    for feed in feeds:
        api_key = FEED1_API_KEY if feed == 1 else FEED2_API_KEY
        if not api_key and args.check_monetization:
            print(f"Feed {feed}: skipped (no API key for monetization)")
            continue
        calls = upsert_run_merchants_into_monthly_log(
            service,
            KELKOO_SHEETS_SPREADSHEET_ID,
            args.run_date,
            feed,
            api_key=api_key,
            check_monetization=args.check_monetization,
        )
        calls_total += int(calls)
        print(f"Feed {feed}: done (Kelkoo calls={calls})")

    print()
    print(f"Total Kelkoo link calls: {calls_total}")


if __name__ == "__main__":
    main()

