#!/usr/bin/env python3
"""
After offers are updated in Keitaro, resume Zeropark campaigns for the same countries.

- Reads the list of countries (geos) from today's offers sheets: YYYY-MM-DD_offers_1 and YYYY-MM-DD_offers_2 (column A).
- Reads the "Zeropark Campaigns" sheet in the same Google spreadsheet: column A = Country (geo), column B = Campaign ID (UUID).
- For each geo that had offers updated, if a Zeropark campaign ID is set, calls Zeropark API to resume that campaign.

The "Zeropark Campaigns" sheet is created automatically if it does not exist (with headers Country, Campaign ID).
Fill in the campaign IDs per country in that sheet.

Requires: KEYZP in .env (Zeropark API token), credentials.json, spreadsheet shared with service account.

  python resume_zeropark_campaigns.py
  python resume_zeropark_campaigns.py --date 2026-03-11
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv()

from config import KEYZP
from integrations.zeropark import resume_campaign, ZeroparkClientError

SPREADSHEET_ID = "1XUkQoWqnNRqaSEnFVRAV36-oi9ENrNWtH5Ct8M4vNuU"
ZEROPARK_SHEET_NAME = "Zeropark Campaigns"


def get_credentials_path():
    p = Path(__file__).resolve().parent / "credentials.json"
    if not p.exists():
        raise FileNotFoundError(f"credentials.json not found at {p}")
    return str(p)


def get_sheets_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    creds = service_account.Credentials.from_service_account_file(get_credentials_path())
    return build("sheets", "v4", credentials=creds).spreadsheets()


def ensure_zeropark_sheet(service) -> None:
    """Create 'Zeropark Campaigns' sheet with headers if it does not exist."""
    meta = service.get(spreadsheetId=SPREADSHEET_ID, fields="sheets(properties(title))").execute()
    titles = [s.get("properties", {}).get("title") for s in meta.get("sheets", [])]
    if ZEROPARK_SHEET_NAME not in titles:
        service.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": ZEROPARK_SHEET_NAME}}}]},
        ).execute()
        quoted = ZEROPARK_SHEET_NAME.replace("'", "''")
        service.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{quoted}'!A1:B1",
            valueInputOption="RAW",
            body={"values": [["Country", "Campaign ID"]]},
        ).execute()
        print(f"Created sheet '{ZEROPARK_SHEET_NAME}' with headers.")
    # Ensure headers exist (no-op if already present)
    quoted = ZEROPARK_SHEET_NAME.replace("'", "''")
    try:
        service.values().get(spreadsheetId=SPREADSHEET_ID, range=f"'{quoted}'!A1:B1").execute()
    except Exception:
        pass


def read_zeropark_mapping(service) -> dict:
    """Read Zeropark Campaigns sheet: A=Country, B=Campaign ID. Return {geo_lower: campaign_id}."""
    quoted = ZEROPARK_SHEET_NAME.replace("'", "''")
    result = service.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{quoted}'!A:B",
    ).execute()
    rows = result.get("values") or []
    mapping = {}
    for row in rows:
        if len(row) < 2:
            continue
        geo = (row[0] or "").strip().lower()
        campaign_id = (row[1] or "").strip()
        if geo in ("country", "geo", ""):
            continue
        if geo and campaign_id:
            mapping[geo] = campaign_id
    return mapping


def get_geos_from_offers_sheets(service, date_str: str) -> set:
    """Return set of country codes (column A) from date_offers_1 and date_offers_2, excluding headers."""
    geos = set()
    for sheet_suffix in ("_offers_1", "_offers_2"):
        sheet_name = f"{date_str}{sheet_suffix}"
        quoted = sheet_name.replace("'", "''")
        try:
            result = service.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{quoted}'!A:A",
            ).execute()
        except Exception:
            continue
        rows = result.get("values") or []
        for i, row in enumerate(rows):
            if not row:
                continue
            geo = (row[0] or "").strip().lower()
            if i == 0 and geo in ("country", "geo", "feed"):
                continue
            if geo and len(geo) <= 4:
                geos.add(geo)
    return geos


def main():
    if not KEYZP:
        print("Error: Set KEYZP in .env (Zeropark API token).")
        sys.exit(1)

    argv = sys.argv[1:]
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    i = 0
    while i < len(argv):
        if argv[i] == "--date" and i + 1 < len(argv):
            date_str = argv[i + 1].strip()
            i += 2
            continue
        i += 1

    try:
        service = get_sheets_service()
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)

    ensure_zeropark_sheet(service)
    mapping = read_zeropark_mapping(service)
    if not mapping:
        print(f"No campaign IDs found in '{ZEROPARK_SHEET_NAME}'. Fill column A (Country) and B (Campaign ID) then re-run.")
        sys.exit(0)

    geos = get_geos_from_offers_sheets(service, date_str)
    if not geos:
        print(f"No countries found in {date_str}_offers_1 / _offers_2. Nothing to resume.")
        sys.exit(0)

    print(f"Resuming Zeropark campaigns for date {date_str}")
    print(f"Countries with offers: {sorted(geos)}")
    print(f"Zeropark mapping: {len(mapping)} countries with campaign ID")
    print()

    ok = 0
    skip = 0
    err = 0
    for geo in sorted(geos):
        campaign_id = mapping.get(geo)
        if not campaign_id:
            print(f"  {geo}: no campaign ID in sheet, skip")
            skip += 1
            continue
        try:
            resume_campaign(campaign_id, KEYZP)
            print(f"  {geo}: resumed {campaign_id[:8]}...")
            ok += 1
        except ZeroparkClientError as e:
            print(f"  {geo}: ERROR {e}")
            if e.response_body:
                print(f"      {e.response_body[:200]}")
            err += 1

    print()
    print(f"Done. Resumed {ok}, skipped {skip}, errors {err}.")


if __name__ == "__main__":
    main()
