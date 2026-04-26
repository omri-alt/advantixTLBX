#!/usr/bin/env python3
"""
Create / extend Google Sheets columns for EC + SK optimizers.

- EC workbook (``EC_SHEETS_SPREADSHEET_ID``): tabs ``trackExploration`` and ``trackWL``
  get a ``budgetReachedYesterday`` column on row 1 if missing (appended; existing data untouched).
- SK workbook (``SK_OPTIMIZER_SHEET_ID``): tabs ``SKtrackExploration`` and ``SKtrackWL``
  are created if missing, or row 1 is extended with any missing headers from the optimizer spec.
- EC + SK **tools** workbook: a ``logs`` tab (exploration / bulk audit) is created if missing.

Requires ``GOOGLE_APPLICATION_CREDENTIALS`` or ``credentials.json`` and Sheets API access.

Usage:
  python cli/setup_optimizer_sheets.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        load_dotenv = None  # type: ignore[assignment]
    if load_dotenv:
        load_dotenv(ROOT / ".env")

    from config import EC_SHEETS_SPREADSHEET_ID, SK_OPTIMIZER_SHEET_ID, SK_TOOLS_SPREADSHEET_ID
    from integrations.autoserver.exploration_sheet_logs import ensure_logs_worksheet
    from integrations.autoserver.sk_optimizer import HEADERS_EXPLORATION, HEADERS_WL

    try:
        from gspread.exceptions import WorksheetNotFound
        from integrations.autoserver import gdocs_as as gd
    except (ImportError, ModuleNotFoundError) as e:
        if "gspread" in str(e).lower():
            print(
                "Missing dependency: gspread (needed for Google Sheets).\n"
                "Install project requirements from the repo root:\n"
                "  pip install -r requirements.txt\n"
                "Or only:\n"
                "  pip install \"gspread>=6,<7\"",
                file=sys.stderr,
            )
            sys.exit(1)
        raise

    ec_id = (EC_SHEETS_SPREADSHEET_ID or "").strip()
    sk_id = (SK_OPTIMIZER_SHEET_ID or "").strip()
    tools_id = (SK_TOOLS_SPREADSHEET_ID or "").strip()
    if not ec_id:
        print("EC_SHEETS_SPREADSHEET_ID is not set; skipping EC tabs.")
    if not sk_id:
        print("SK_OPTIMIZER_SHEET_ID is not set; skipping SK tabs.")
    if not tools_id:
        print("SK_TOOLS_SPREADSHEET_ID is not set; skipping SK tools logs tab.")

    extra = ["budgetReachedYesterday"]

    if ec_id:
        for tab in ("trackExploration", "trackWL"):
            try:
                added = gd.append_missing_headers_row1(
                    ec_id, tab, extra, create_if_missing=False
                )
            except WorksheetNotFound:
                print(f"EC {tab!r}: worksheet not found — create the tab first; skipped.")
                continue
            print(f"EC {tab!r}: appended headers {added or '(none)'}")
        try:
            ensure_logs_worksheet(ec_id)
            print("EC logs: ensured tab 'logs' exists.")
        except Exception as e:
            print(f"EC logs tab: {e}")

    if sk_id:
        added_e = gd.append_missing_headers_row1(sk_id, "SKtrackExploration", HEADERS_EXPLORATION)
        print(f"SK SKtrackExploration: new/updated headers {added_e or '(already complete)'}")
        added_w = gd.append_missing_headers_row1(sk_id, "SKtrackWL", HEADERS_WL)
        print(f"SK SKtrackWL: new/updated headers {added_w or '(already complete)'}")

    if tools_id:
        try:
            ensure_logs_worksheet(tools_id)
            print("SK tools workbook: ensured tab 'logs' exists.")
        except Exception as e:
            print(f"SK tools logs tab: {e}")

    print("Done.")


if __name__ == "__main__":
    main()
