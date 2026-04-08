#!/usr/bin/env python3
"""
Generate potential merchants list from Kelkoo aggregated reports and write to the Blend spreadsheet.

Outputs are intended to be *feed-specific* sheets:
  - potentialKelkoo1
  - potentialKelkoo2

Defaults:
  - shows BOTH monetized and unmonetized merchants (column `kelkoo_monetization`)
  - conversion-rate column is `cr` as a percent string (e.g. "1.23%")
  - thresholds:
      - Static merchants: CR >= 0.3%
      - Flex merchants:   CR >= 1.0%

Usage:
  python blend_potential_merchants.py --feed kelkoo1
  python blend_potential_merchants.py --feed kelkoo2
  python blend_potential_merchants.py --feed kelkoo1 --only-monetized
  python blend_potential_merchants.py --feed kelkoo1 --start 2026-03-01 --end 2026-03-25
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv

load_dotenv()

from config import BLEND_SHEETS_SPREADSHEET_ID, FEED1_API_KEY, FEED2_API_KEY
from workflows.kelkoo_daily import download_merchants_feed, REPORTS_AGGREGATED_URL, _headers
from integrations.kelkoo_search import kelkoo_merchant_link_check, format_kelkoo_monetization_status

BLEND_SPREADSHEET_ID = BLEND_SHEETS_SPREADSHEET_ID


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


def _month_to_yesterday_range() -> Tuple[str, str]:
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    if today.day == 1:
        start = yesterday.replace(day=1)
        end = yesterday
    else:
        start = today.replace(day=1)
        end = yesterday
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _api_key_for_feed(feed: str) -> str:
    f = (feed or "").strip().lower()
    if f == "kelkoo1":
        return FEED1_API_KEY
    if f == "kelkoo2":
        return FEED2_API_KEY
    return ""


def _default_output_sheet(feed: str) -> str:
    f = (feed or "").strip().lower()
    return "potentialKelkoo1" if f == "kelkoo1" else "potentialKelkoo2"


def _cr_percent_str(sales: int, leads: int) -> str:
    cr = (sales / max(leads, 1)) * 100.0
    return f"{cr:.2f}%"


def _is_static_tier(merchant_tier: str) -> bool:
    return (merchant_tier or "").strip().lower() == "static"


def ensure_sheet(service, title: str) -> None:
    meta = service.get(spreadsheetId=BLEND_SPREADSHEET_ID, fields="sheets(properties(title))").execute()
    titles = [s.get("properties", {}).get("title") for s in meta.get("sheets", [])]
    if title not in titles:
        service.batchUpdate(
            spreadsheetId=BLEND_SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
        ).execute()


def write_sheet(service, title: str, rows: List[List[str]]) -> None:
    ensure_sheet(service, title)
    quoted = title.replace("'", "''")
    service.values().clear(spreadsheetId=BLEND_SPREADSHEET_ID, range=f"'{quoted}'!A1:Z50000").execute()
    service.values().update(
        spreadsheetId=BLEND_SPREADSHEET_ID,
        range=f"'{quoted}'!A1",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--feed", required=True, choices=["kelkoo1", "kelkoo2"])
    p.add_argument("--output", default=None, help="Output sheet name (default: potentialKelkoo1/2)")
    p.add_argument("--start", default=None)
    p.add_argument("--end", default=None)
    p.add_argument("--only-monetized", action="store_true", help="Hide unmonetized rows")
    args = p.parse_args()

    api_key = _api_key_for_feed(args.feed)
    if not api_key:
        print(f"Error: API key missing for {args.feed}", file=sys.stderr)
        sys.exit(1)

    start, end = (args.start, args.end)
    if not start or not end:
        start, end = _month_to_yesterday_range()

    out_sheet = args.output or _default_output_sheet(args.feed)
    only_monetized = bool(args.only_monetized)

    print(f"Kelkoo reports ({args.feed}): {start} -> {end}")
    r = requests.get(
        REPORTS_AGGREGATED_URL,
        params={"start": start, "end": end, "groupBy": "merchantId", "format": "JSON"},
        headers=_headers(api_key),
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Reports API {r.status_code}: {r.text[:500]}")
    report_items = r.json() or []

    merchants_feed = download_merchants_feed(api_key, static_only=False)
    feed_by_id: Dict[str, Dict[str, str]] = {}
    for m in merchants_feed:
        keys: List[str] = []
        if m.get("id") is not None:
            keys.append(str(m.get("id")))
        if m.get("websiteId") is not None:
            keys.append(str(m.get("websiteId")))
        if not keys:
            continue
        info = {
            "name": str(m.get("name") or "").strip(),
            "domain": str(m.get("url") or "").strip(),
            "geo_origin": str(m.get("geo_origin") or "").strip().lower()[:2],
            "merchantTier": str(m.get("merchantTier") or "").strip(),
        }
        for k in keys:
            if k not in feed_by_id:
                feed_by_id[k] = info

    header = [
        "merchantId",
        "merchant",
        "domain",
        "geo_origin",
        "leads",
        "sales",
        "cr",
        "merchantTier",
        "kelkoo_monetization",
    ]
    rows_out: List[List[str]] = []
    checked = 0

    for item in report_items:
        mid = item.get("merchantId")
        if mid is None:
            continue
        mid = str(mid)
        leads = int(item.get("leadCount") or 0)
        sales = int(item.get("saleCount") or 0)
        cr = sales / max(leads, 1)

        info = feed_by_id.get(mid) or {}
        tier = (info.get("merchantTier") or "").strip() or "Flex"
        min_cr = 0.003 if _is_static_tier(tier) else 0.01
        if cr < min_cr:
            continue

        merchant = (info.get("name") or "").strip() or str(item.get("merchantName") or "").strip()
        domain = (info.get("domain") or "").strip()
        geo_origin = (info.get("geo_origin") or "").strip()

        geo2 = (geo_origin or "").strip().lower()[:2]
        if not domain:
            monetization = "no_merchant_url"
        elif len(geo2) != 2:
            monetization = "bad_geo"
        else:
            checked += 1
            monetization = format_kelkoo_monetization_status(kelkoo_merchant_link_check(domain, geo2, api_key))

        is_monetized = monetization.startswith("monetized")
        if only_monetized and not is_monetized:
            continue

        rows_out.append([
            mid,
            merchant,
            domain,
            geo_origin,
            str(leads),
            str(sales),
            _cr_percent_str(sales, leads),
            tier,
            monetization,
        ])

    # Sort by CR desc, then sales desc, then leads desc
    def sort_key(r: List[str]):
        cr_num = float(r[6].replace("%", "")) if r[6].endswith("%") else 0.0
        return (-cr_num, -int(r[5]), -int(r[4]))

    rows_out.sort(key=sort_key)
    out = [header] + rows_out

    service = get_sheets_service()
    write_sheet(service, out_sheet, out)
    print(f"Wrote {len(rows_out)} rows to {out_sheet!r}. Checked={checked}. only_monetized={only_monetized}")


if __name__ == "__main__":
    main()

