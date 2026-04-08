#!/usr/bin/env python3
"""
Fill column **E — Kelkoo monetization** on ``{month}_log_1`` / ``{month}_log_2`` tabs.

Uses the same Kelkoo link check as ``monetization_check.py`` (URL from merchants feed +
2-letter geo). By default only rows with an **empty** column E are updated; use ``--force``
to re-check all rows (optionally scoped with ``--run-date``).

  python enrich_monthly_log_monetization.py --year-month 2026-03
  python enrich_monthly_log_monetization.py --year-month 2026-03 --feed 1
  python enrich_monthly_log_monetization.py --year-month 2026-03 --force
  python enrich_monthly_log_monetization.py --year-month 2026-03 --run-date 2026-03-20

Requires ``FEED1_API_KEY`` / ``FEED2_API_KEY``, ``credentials.json``, ``KELKOO_SHEETS_SPREADSHEET_ID``.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv

load_dotenv()

from config import FEED1_API_KEY, FEED2_API_KEY, KELKOO_SHEETS_SPREADSHEET_ID
from workflows.monthly_log_monetization import (
    build_merchant_geo_url_lookup,
    count_enrich_candidates,
    enrich_log_rows_monetization,
    month_log_sheet_title,
    read_sheet_values_raw,
    write_full_log_sheet,
)


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
    parser = argparse.ArgumentParser(description="Add Kelkoo monetization column E to monthly merchant logs.")
    parser.add_argument("--year-month", dest="year_month", metavar="YYYY-MM", help="Month of the log tab (default: current UTC month)")
    parser.add_argument("--feed", choices=["1", "2", "both"], default="both")
    parser.add_argument("--force", action="store_true", help="Re-check every row (overwrite column E)")
    parser.add_argument("--run-date", dest="run_date", metavar="YYYY-MM-DD", help="Only update rows with this Run date (column A)")
    parser.add_argument("--dry-run", action="store_true", help="Print how many rows would be checked; no sheet write")
    args = parser.parse_args()

    if args.year_month:
        try:
            y, m = args.year_month.strip().split("-", 1)
            year, month = int(y), int(m)
            if not (1 <= month <= 12):
                raise ValueError
        except ValueError:
            print("Error: --year-month must be YYYY-MM", file=sys.stderr)
            sys.exit(1)
    else:
        now = datetime.now(timezone.utc)
        year, month = now.year, now.month

    feeds = [1, 2] if args.feed == "both" else [int(args.feed)]

    try:
        service = get_sheets_service()
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    print(f"Spreadsheet: {KELKOO_SHEETS_SPREADSHEET_ID}")
    print(f"Month: {year}-{month:02d}, feeds: {feeds}, force={args.force}, run_date={args.run_date or 'all'}")
    print()

    for feed in feeds:
        api_key = FEED1_API_KEY if feed == 1 else FEED2_API_KEY
        if not (api_key or "").strip():
            print(f"Feed {feed}: skipped (no API key)")
            continue
        log_name = month_log_sheet_title(year, month, feed)
        rows = read_sheet_values_raw(service, KELKOO_SHEETS_SPREADSHEET_ID, log_name, "A:Z")
        if not rows:
            print(f"Feed {feed}: tab {log_name!r} missing or empty — nothing to enrich.")
            continue

        if args.dry_run:
            c = count_enrich_candidates(rows, only_run_date=args.run_date, force=args.force)
            print(f"Feed {feed} ({log_name}): would run {c} Kelkoo link checks (dry-run)")
            continue

        by_geo_id, by_id = build_merchant_geo_url_lookup(api_key)
        new_rows, calls = enrich_log_rows_monetization(
            rows,
            api_key,
            by_geo_id,
            by_id,
            only_run_date=args.run_date,
            force=args.force,
        )
        print(f"Feed {feed} ({log_name}): {calls} Kelkoo link checks")
        write_full_log_sheet(service, KELKOO_SHEETS_SPREADSHEET_ID, log_name, new_rows)
        print(f"  Wrote {len(new_rows) - 1} data rows.")

    print("Done.")


if __name__ == "__main__":
    main()
