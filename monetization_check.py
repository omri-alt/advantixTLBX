#!/usr/bin/env python3
"""
Monetization checker (Kelkoo feed1/2, Yadore feed3, Adexa feed4) driven by Google Sheets.

Spreadsheet: 1z1Y-vPuqk6zI673ytgBQvoQNnqMosFeZkdAiOMMPgM0
Input sheet: sourceToCheck (columns: url, geo)
Output sheet: Matches

For each (url, geo):
  - Kelkoo link check (feed1/2): GET …/search/link
  - Yadore deeplink (feed3): POST /v2/deeplink with ``isCouponing`` false and true
  - Adexa Link Monetizer (feed4): GET …/LinksMerchant.php

Writes one row per input line with statuses and CPC fields where available.

Usage:
  python monetization_check.py
"""
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv()

from config import FEED1_API_KEY, FEED2_API_KEY
from integrations.kelkoo_search import kelkoo_merchant_link_check as kelkoo_check
from integrations.yadore import deeplink as yadore_deeplink, YadoreClientError
from integrations.adexa import links_merchant_check as adexa_links_check, AdexaClientError
from integrations.monetization_geo import yadore_feed_class

SPREADSHEET_ID = "1z1Y-vPuqk6zI673ytgBQvoQNnqMosFeZkdAiOMMPgM0"
INPUT_SHEET = "sourceToCheck"
OUTPUT_SHEET = "Matches"

# Parallel HTTP calls per row (Kelkoo×2 + Yadore×2 + Adexa).
_ROW_POOL_WORKERS = 5


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


def _run_row_checks(url: str, geo: str) -> Dict[str, Any]:
    """Run all feed checks for one row in parallel (same network time ≈ one slowest call)."""

    def _k1() -> Dict[str, Any]:
        return kelkoo_check(url, geo, FEED1_API_KEY)

    def _k2() -> Dict[str, Any]:
        return kelkoo_check(url, geo, FEED2_API_KEY)

    def _ync() -> Dict[str, Any]:
        try:
            return yadore_deeplink(url, geo, is_couponing=False)
        except YadoreClientError:
            return {"found": False, "estimatedCpc_amount": "", "estimatedCpc_currency": ""}

    def _yc() -> Dict[str, Any]:
        try:
            return yadore_deeplink(url, geo, is_couponing=True)
        except YadoreClientError:
            return {"found": False, "estimatedCpc_amount": "", "estimatedCpc_currency": ""}

    def _ax() -> Dict[str, Any]:
        try:
            return adexa_links_check(url, geo)
        except AdexaClientError as e:
            return {"found": False, "note": str(e)[:200]}

    futures = {}
    with ThreadPoolExecutor(max_workers=_ROW_POOL_WORKERS) as ex:
        futures[ex.submit(_k1)] = "k1"
        futures[ex.submit(_k2)] = "k2"
        futures[ex.submit(_ync)] = "ync"
        futures[ex.submit(_yc)] = "yc"
        futures[ex.submit(_ax)] = "ax"

    out: Dict[str, Any] = {}
    for fut in as_completed(futures):
        key = futures[fut]
        try:
            out[key] = fut.result()
        except Exception as e:
            if key == "k1" or key == "k2":
                out[key] = {"found": False, "estimatedCpc": ""}
            elif key in ("ync", "yc"):
                out[key] = {"found": False, "estimatedCpc_amount": "", "estimatedCpc_currency": ""}
            else:
                out[key] = {"found": False, "note": str(e)[:200]}

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
        "adexa_found",
        "adexa_note",
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
        r = _run_row_checks(url, geo)
        k1 = r["k1"]
        k2 = r["k2"]
        y_nc = r["ync"]
        y_c = r["yc"]
        ax = r["ax"]

        y_nc_found = bool(y_nc.get("found"))
        y_nc_cpc = y_nc.get("estimatedCpc_amount") or ""
        y_nc_cur = y_nc.get("estimatedCpc_currency") or ""
        y_c_found = bool(y_c.get("found"))
        y_c_cpc = y_c.get("estimatedCpc_amount") or ""
        y_c_cur = y_c.get("estimatedCpc_currency") or ""

        y_class = yadore_feed_class(y_nc_found, y_c_found)

        ax_found = bool(ax.get("found"))
        ax_note = str(ax.get("note") or "")

        out_rows.append(
            [
                ts,
                url,
                geo,
                y_class,
                str(y_nc_found),
                str(y_c_found),
                str(ax_found),
                ax_note,
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

