#!/usr/bin/env python3
"""One-off: list spreadsheet tabs + sample A1:Z5 for keyword / dated / log sheets.

  python tools/inspect_kelkoo_sheet_tabs.py
  python tools/inspect_kelkoo_sheet_tabs.py --late-sales
  python tools/inspect_kelkoo_sheet_tabs.py --spreadsheet-id <id>
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from config import KELKOO_LATE_SALES_SPREADSHEET_ID, KELKOO_SHEETS_SPREADSHEET_ID  # noqa: E402
from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="List Google Sheet tabs and sample rows.")
    ap.add_argument(
        "--spreadsheet-id",
        dest="spreadsheet_id",
        default="",
        help="Override spreadsheet id (default: Nipuhim notebook unless --late-sales).",
    )
    ap.add_argument(
        "--late-sales",
        action="store_true",
        help="Use KELKOO_LATE_SALES_SPREADSHEET_ID (KLtools late-sales report; both feeds).",
    )
    args = ap.parse_args()

    creds_path = ROOT / "credentials.json"
    if not creds_path.exists():
        print("ERROR: credentials.json not found at", creds_path)
        sys.exit(1)

    if (args.spreadsheet_id or "").strip():
        sid = args.spreadsheet_id.strip()
    elif args.late_sales:
        sid = KELKOO_LATE_SALES_SPREADSHEET_ID
    else:
        sid = KELKOO_SHEETS_SPREADSHEET_ID
    print("spreadsheet_id:", sid)

    creds = service_account.Credentials.from_service_account_file(str(creds_path))
    service = build("sheets", "v4", credentials=creds).spreadsheets()

    meta = service.get(
        spreadsheetId=sid,
        fields="properties(title),sheets(properties(sheetId,title))",
    ).execute()
    print("Spreadsheet title:", (meta.get("properties") or {}).get("title"))
    sheets = meta.get("sheets") or []
    print("TAB_COUNT:", len(sheets))
    print("--- All tabs (gid, title) ---")
    for s in sheets:
        p = s.get("properties") or {}
        print(p.get("sheetId"), "\t", repr(p.get("title")))

    kw = re.compile(r"sale|click|late|today|7.?day|conv|postback|lead|mtd", re.I)
    interesting: list[str] = []
    for s in sheets:
        t = (s.get("properties") or {}).get("title") or ""
        if kw.search(t):
            interesting.append(t)

    dated: list[str] = []
    for s in sheets:
        t = (s.get("properties") or {}).get("title") or ""
        if re.match(r"\d{4}-\d{2}-\d{2}", t):
            dated.append(t)
    dated = sorted(set(dated), reverse=True)[:12]

    log_tabs = []
    for s in sheets:
        t = (s.get("properties") or {}).get("title") or ""
        if t and "_log_" in t.lower():
            log_tabs.append(t)
    to_sample = list(dict.fromkeys(interesting + dated + log_tabs))[:35]

    def q(title: str) -> str:
        return "'" + title.replace("'", "''") + "'"

    if not to_sample:
        to_sample = [(s.get("properties") or {}).get("title") for s in sheets[:8]]
        to_sample = [t for t in to_sample if t]

    ranges = [f"{q(t)}!A1:Z5" for t in to_sample if t]
    print("\n--- Sampling", len(ranges), "tabs (A1:Z5) ---")
    body = service.values().batchGet(spreadsheetId=sid, ranges=ranges).execute()
    for vr in body.get("valueRanges") or []:
        rng = vr.get("range", "")
        vals = vr.get("values") or []
        print("\n===", rng, "===")
        for i, row in enumerate(vals):
            print(i, row[:18])


if __name__ == "__main__":
    main()
