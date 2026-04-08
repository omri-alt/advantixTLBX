#!/usr/bin/env python3
"""
Monetization checker (Kelkoo feed1 + Yadore feed3) driven by Google Sheets.

Spreadsheet: 1z1Y-vPuqk6zI673ytgBQvoQNnqMosFeZkdAiOMMPgM0
Input sheet: sourceToCheck (columns: url, geo)
Output sheet: Matches

For each (url, geo):
  - Kelkoo link check: GET /publisher/shopping/v2/search/link?country=..&merchantUrl=..
  - Yadore deeplink: POST /v2/deeplink

Writes one row per input line with statuses and deeplink details.

Usage:
  python monetization_check.py
"""
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv()

from config import FEED1_API_KEY, FEED2_API_KEY
from integrations.kelkoo_search import kelkoo_merchant_link_check as kelkoo_check
from integrations.yadore import deeplink as yadore_deeplink, YadoreClientError

SPREADSHEET_ID = "1z1Y-vPuqk6zI673ytgBQvoQNnqMosFeZkdAiOMMPgM0"
INPUT_SHEET = "sourceToCheck"
OUTPUT_SHEET = "Matches"


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


def ensure_sheet(service, title: str, header: List[str]) -> None:
    meta = service.get(spreadsheetId=SPREADSHEET_ID, fields="sheets(properties(title))").execute()
    titles = [s.get("properties", {}).get("title") for s in meta.get("sheets", [])]
    if title not in titles:
        service.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
        ).execute()
    quoted = title.replace("'", "''")
    service.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{quoted}'!A1",
        valueInputOption="RAW",
        body={"values": [header]},
    ).execute()


def read_source_rows(service) -> List[Tuple[str, str]]:
    quoted = INPUT_SHEET.replace("'", "''")
    result = service.values().get(spreadsheetId=SPREADSHEET_ID, range=f"'{quoted}'!A:Z").execute()
    rows = result.get("values") or []
    if not rows:
        return []
    header = [str(c or "").strip().lower() for c in rows[0]]
    idx_url = header.index("url") if "url" in header else 0
    idx_geo = header.index("geo") if "geo" in header else 1

    out: List[Tuple[str, str]] = []
    seen = set()
    for row in rows[1:]:
        url = (row[idx_url] if idx_url < len(row) else "") or ""
        geo = (row[idx_geo] if idx_geo < len(row) else "") or ""
        url = str(url).strip()
        geo = str(geo).strip().lower()[:2]
        if not url or not geo:
            continue
        key = (url, geo)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def main() -> None:
    argv = sys.argv[1:]
    max_rows = None
    i = 0
    while i < len(argv):
        if argv[i] == "--max-rows" and i + 1 < len(argv):
            max_rows = int(argv[i + 1])
            i += 2
            continue
        i += 1

    service = get_sheets_service()
    header = [
        "timestamp_utc",
        "url",
        "geo",
        "yadore_monetization",
        "yadore_nc_found",
        "yadore_c_found",
        "kelkoo1_found",
        "kelkoo2_found",
        "yadore_nc_cpc",
        "yadore_nc_currency",
        "yadore_c_cpc",
        "yadore_c_currency",
        "kelkoo1_cpc",
        "kelkoo2_cpc",
    ]
    ensure_sheet(service, OUTPUT_SHEET, header)

    rows = read_source_rows(service)
    if max_rows is not None:
        rows = rows[:max_rows]
    if not rows:
        print("No rows found in sourceToCheck.")
        return

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out_rows = [header]
    total = len(rows)
    for idx, (url, geo) in enumerate(rows, start=1):
        print(f"[{idx}/{total}] {geo} {url[:60]}...")
        k1 = kelkoo_check(url, geo, FEED1_API_KEY)
        k2 = kelkoo_check(url, geo, FEED2_API_KEY)
        # Yadore: check both non-coupon and coupon traffic
        try:
            y_nc = yadore_deeplink(url, geo, is_couponing=False)
            y_nc_found = bool(y_nc.get("found"))
            y_nc_cpc = y_nc.get("estimatedCpc_amount") or ""
            y_nc_cur = y_nc.get("estimatedCpc_currency") or ""
        except YadoreClientError:
            y_nc_found = False
            y_nc_cpc = ""
            y_nc_cur = ""

        try:
            y_c = yadore_deeplink(url, geo, is_couponing=True)
            y_c_found = bool(y_c.get("found"))
            y_c_cpc = y_c.get("estimatedCpc_amount") or ""
            y_c_cur = y_c.get("estimatedCpc_currency") or ""
        except YadoreClientError:
            y_c_found = False
            y_c_cpc = ""
            y_c_cur = ""

        if y_nc_found:
            y_class = "any"
        elif y_c_found:
            y_class = "coupons_only"
        else:
            y_class = "no"

        out_rows.append(
            [
                ts,
                url,
                geo,
                y_class,
                str(y_nc_found),
                str(y_c_found),
                str(bool(k1.get("found"))),
                str(bool(k2.get("found"))),
                str(y_nc_cpc),
                str(y_nc_cur),
                str(y_c_cpc),
                str(y_c_cur),
                str(k1.get("estimatedCpc", "")),
                str(k2.get("estimatedCpc", "")),
            ]
        )

    quoted = OUTPUT_SHEET.replace("'", "''")
    service.values().clear(spreadsheetId=SPREADSHEET_ID, range=f"'{quoted}'!A1:Z10000").execute()
    service.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{quoted}'!A1",
        valueInputOption="RAW",
        body={"values": out_rows},
    ).execute()
    print(f"Wrote {len(out_rows) - 1} rows to {OUTPUT_SHEET}.")


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

