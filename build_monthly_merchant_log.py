#!/usr/bin/env python3
"""
Build monthly merchant logs from dated *offers* sheets in the Kelkoo Google Sheet.

Scans all tabs named ``YYYY-MM-DD_offers_1`` / ``YYYY-MM-DD_offers_2`` for a given
calendar month, dedupes by (run date, country, merchant id), resolves **merchant name**
(fixim tab if still present, else Kelkoo merchants feed + aggregated report for that month),
and writes:

  ``{month}_log_1``  e.g. ``march_log_1``
  ``{month}_log_2``  e.g. ``march_log_2``

Columns: Run date, Country, Merchant ID, Merchant name, Kelkoo monetization (E left blank; run
``enrich_monthly_log_monetization.py`` or the daily workflow to fill).

Names are resolved from (in order): same-day fixim tab, live Kelkoo merchants feed, then
aggregated **month-to-date** report for that calendar month (``merchantName``), using the
same feed API key as the offers tab (feed 1 vs 2).

Requires credentials.json and ``KELKOO_SHEETS_SPREADSHEET_ID`` in .env (or config default).

  python build_monthly_merchant_log.py
  python build_monthly_merchant_log.py --year-month 2026-03
  python build_monthly_merchant_log.py --dry-run
"""
from __future__ import annotations

import argparse
import re
import sys
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv

load_dotenv()

from config import FEED1_API_KEY, FEED2_API_KEY, KELKOO_SHEETS_SPREADSHEET_ID
from workflows.kelkoo_daily import (
    build_merchant_id_to_name_from_feed,
    fetch_report_merchant_names,
)

_OFFERS_SHEET_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})_offers_([12])$")

# Fixim tab column names (Kelkoo merchants JSON → sheet headers, case varies)
_FIXIM_NAME_HEADER_CANDIDATES = (
    "name",
    "merchantname",
    "merchant_name",
    "shopname",
    "shop_name",
    "merchanttitle",
    "title",
    "domain",
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


def month_log_sheet_title(year: int, month: int, feed: int) -> str:
    """e.g. march_log_1 (English month name, lowercase)."""
    name = date(year, month, 1).strftime("%B").lower()
    return f"{name}_log_{feed}"


def _norm_header(h: str) -> str:
    return (h or "").strip().lower()


def _col_index(headers: List[str], *candidates: str) -> int:
    lowered = [_norm_header(h) for h in headers]
    for c in candidates:
        key = c.strip().lower()
        if key in lowered:
            return lowered.index(key)
    return -1


def _find_fixim_name_column(headers: List[str]) -> int:
    """Index of best column for merchant display name on fixim sheet."""
    compact = [_norm_header(h).replace(" ", "").replace("_", "") for h in headers]
    for cand in _FIXIM_NAME_HEADER_CANDIDATES:
        c = cand.replace(" ", "").replace("_", "")
        if c in compact:
            return compact.index(c)
    for i, h in enumerate(compact):
        if "name" in h and "product" not in h and h not in ("merchantid", "id", "geo_origin"):
            return i
    return -1


def parse_fixim_name_lookup(values: List[List[Any]]) -> Tuple[Dict[Tuple[str, str], str], Dict[str, str]]:
    """
    From a fixim sheet: maps (country_upper, merchant_id) -> name, and merchant_id -> name (any geo).
    """
    by_geo_id: Dict[Tuple[str, str], str] = {}
    by_id: Dict[str, str] = {}
    if not values or len(values) < 2:
        return by_geo_id, by_id
    headers = values[0]
    id_i = _col_index(headers, "id")
    geo_i = _col_index(headers, "geo_origin", "geo origin")
    name_i = _find_fixim_name_column(headers)
    if id_i < 0 or name_i < 0:
        return by_geo_id, by_id

    for row in values[1:]:
        if id_i >= len(row) or name_i >= len(row):
            continue
        mid = str(row[id_i] or "").strip()
        if not mid:
            continue
        raw_name = str(row[name_i] or "").strip()
        if not raw_name:
            continue
        geo_u = ""
        if geo_i >= 0 and geo_i < len(row):
            geo_u = str(row[geo_i] or "").strip().upper()
        if geo_u:
            by_geo_id.setdefault((geo_u, mid), raw_name)
        by_id.setdefault(mid, raw_name)

    return by_geo_id, by_id


def lookup_merchant_name(
    by_geo_id: Dict[Tuple[str, str], str],
    by_id: Dict[str, str],
    country: str,
    merchant_id: str,
) -> str:
    c = country.upper().strip()
    return (by_geo_id.get((c, merchant_id)) or by_id.get(merchant_id, "")).strip()


def extract_merchant_rows_from_offers(
    sheet_title: str,
    values: List[List[Any]],
) -> List[Tuple[str, str, str]]:
    """
    Return list of (run_date, country, merchant_id) unique rows for one offers sheet.
    run_date comes from the tab name YYYY-MM-DD_offers_N.
    """
    m = _OFFERS_SHEET_RE.match(sheet_title)
    if not m or not values or len(values) < 2:
        return []
    run_date = m.group(1)
    headers = values[0]
    country_i = _col_index(headers, "country")
    mid_i = _col_index(headers, "merchant id", "merchant_id", "merchantid")
    if country_i < 0:
        country_i = 0
    if mid_i < 0:
        mid_i = 1

    seen: Set[Tuple[str, str, str]] = set()
    out: List[Tuple[str, str, str]] = []
    for row in values[1:]:
        if country_i >= len(row) or mid_i >= len(row):
            continue
        country = str(row[country_i] or "").strip()
        mid = str(row[mid_i] or "").strip()
        if not country or not mid:
            continue
        key = (run_date, country.upper(), mid)
        if key in seen:
            continue
        seen.add(key)
        out.append((run_date, country.upper(), mid))
    return out


def report_date_range_for_month(year: int, month: int) -> Optional[Tuple[str, str]]:
    """
    Kelkoo report window for the log month: 1st of month through min(last day, yesterday).

    Returns None if the whole month is still in the future (no API range).
    """
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    start_d = date(year, month, 1)
    last_d = date(year, month, monthrange(year, month)[1])
    end_d = min(last_d, yesterday)
    if end_d < start_d:
        return None
    return start_d.isoformat(), end_d.isoformat()


def build_kelkoo_name_lookup_for_feed(api_key: str, year: int, month: int) -> Dict[str, str]:
    """
    Merge merchants-feed names with report ``merchantName`` for the month (report fills gaps).
    Feed wins on key collision.
    """
    if not (api_key or "").strip():
        return {}
    report_map: Dict[str, str] = {}
    rng = report_date_range_for_month(year, month)
    if rng:
        start_s, end_s = rng
        try:
            report_map = fetch_report_merchant_names(api_key, start_s, end_s)
        except Exception as e:
            print(f"  Warning: Kelkoo report names ({start_s}..{end_s}): {e}")
    feed_map: Dict[str, str] = {}
    try:
        feed_map = build_merchant_id_to_name_from_feed(api_key)
    except Exception as e:
        print(f"  Warning: Kelkoo merchants feed (names): {e}")
    # Report first, then feed overwrites (feed is authoritative)
    out = dict(report_map)
    out.update(feed_map)
    return out


def list_offers_sheet_titles_for_month(
    all_titles: List[str],
    year: int,
    month: int,
) -> Dict[int, List[str]]:
    """feed -> sorted list of sheet titles in that month."""
    start = date(year, month, 1)
    end = date(year, month, monthrange(year, month)[1])
    by_feed: Dict[int, List[str]] = {1: [], 2: []}
    for title in all_titles:
        m = _OFFERS_SHEET_RE.match(title)
        if not m:
            continue
        ds, feed_s = m.group(1), int(m.group(2))
        try:
            d = datetime.strptime(ds, "%Y-%m-%d").date()
        except ValueError:
            continue
        if start <= d <= end:
            by_feed[feed_s].append(title)
    for f in by_feed:
        by_feed[f].sort()
    return by_feed


def read_values(
    service: Any,
    spreadsheet_id: str,
    sheet_title: str,
    *,
    last_col: str = "ZZ",
) -> List[List[Any]]:
    quoted = sheet_title.replace("'", "''")
    try:
        return (
            service.values()
            .get(spreadsheetId=spreadsheet_id, range=f"'{quoted}'!A:{last_col}")
            .execute()
            .get("values")
            or []
        )
    except Exception:
        return []


def write_log_sheet(
    service: Any,
    spreadsheet_id: str,
    sheet_name: str,
    rows: List[List[Any]],
) -> None:
    """Create or replace contents of a log tab."""
    headers = [
        "Run date",
        "Country",
        "Merchant ID",
        "Merchant name",
        "Kelkoo monetization",
    ]
    # Pad data rows to 5 columns (E empty until enriched)
    padded = []
    for r in rows:
        row = list(r) + [""] * 5
        padded.append(row[:5])
    data = [headers] + padded

    meta = service.get(spreadsheetId=spreadsheet_id, fields="sheets(properties(title))").execute()
    titles = [s["properties"]["title"] for s in meta.get("sheets", [])]
    if sheet_name not in titles:
        service.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]},
        ).execute()

    quoted = sheet_name.replace("'", "''")
    service.values().clear(spreadsheetId=spreadsheet_id, range=f"'{quoted}'!A1:Z50000").execute()
    if len(data) > 1:
        service.values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{quoted}'!A1",
            valueInputOption="USER_ENTERED",
            body={"values": data},
        ).execute()


def build_logs(
    service: Any,
    spreadsheet_id: str,
    year: int,
    month: int,
    *,
    kelkoo_names_by_feed: Optional[Dict[int, Dict[str, str]]] = None,
    dry_run: bool = False,
) -> None:
    meta = service.get(spreadsheetId=spreadsheet_id, fields="sheets(properties(title))").execute()
    all_titles = set(s["properties"]["title"] for s in meta.get("sheets", []))
    by_feed = list_offers_sheet_titles_for_month(list(all_titles), year, month)
    names_by_feed = kelkoo_names_by_feed or {}

    for feed in (1, 2):
        titles = by_feed[feed]
        log_name = month_log_sheet_title(year, month, feed)
        merged: List[Tuple[str, str, str]] = []
        global_seen: Set[Tuple[str, str, str]] = set()
        for t in titles:
            vals = read_values(service, spreadsheet_id, t)
            for trip in extract_merchant_rows_from_offers(t, vals):
                if trip not in global_seen:
                    global_seen.add(trip)
                    merged.append(trip)
        merged.sort(key=lambda x: (x[0], x[1], x[2]))

        # Same-day fixim tab: YYYY-MM-DD_fixim_N → merchant display names
        fixim_maps: Dict[Tuple[str, int], Tuple[Dict[Tuple[str, str], str], Dict[str, str]]] = {}
        run_dates = {row[0] for row in merged}
        for ds in sorted(run_dates):
            fixim_title = f"{ds}_fixim_{feed}"
            if fixim_title not in all_titles:
                fixim_maps[(ds, feed)] = ({}, {})
                continue
            fvals = read_values(service, spreadsheet_id, fixim_title, last_col="ZZ")
            fixim_maps[(ds, feed)] = parse_fixim_name_lookup(fvals)

        print(f"Feed {feed}: {len(titles)} offers sheets in {year}-{month:02d} -> {len(merged)} unique (date,country,merchant) rows -> tab {log_name!r}")
        if dry_run:
            continue

        api_lookup = names_by_feed.get(feed, {})
        table: List[List[Any]] = []
        for run_date, country, mid in merged:
            by_geo_id, by_id = fixim_maps.get((run_date, feed), ({}, {}))
            mname = lookup_merchant_name(by_geo_id, by_id, country, mid)
            if not mname:
                mname = (api_lookup.get(mid) or api_lookup.get(str(mid).strip()) or "").strip()
            table.append([run_date, country, mid, mname, ""])

        write_log_sheet(service, spreadsheet_id, log_name, table)
        named = sum(1 for r in table if r[3])
        print(f"  Wrote {log_name} ({len(table)} data rows + header; {named} with merchant name).")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build {month}_log_1 / _log_2 from dated offers sheets.")
    parser.add_argument(
        "--year-month",
        dest="year_month",
        metavar="YYYY-MM",
        help="Calendar month to aggregate (default: current UTC month)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print counts only; do not write sheets")
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

    try:
        service = get_sheets_service()
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Spreadsheet: {KELKOO_SHEETS_SPREADSHEET_ID}")
    print(f"Month: {year}-{month:02d}")
    rng = report_date_range_for_month(year, month)
    if rng:
        print(f"Kelkoo report window for names: {rng[0]} .. {rng[1]} (aggregated + merchants feed)")
    else:
        print("Kelkoo report window: N/A (month in future); using merchants feed only for names")
    print()
    print("Loading merchant names from Kelkoo (feed + report) ...")
    kelkoo_names: Dict[int, Dict[str, str]] = {}
    if FEED1_API_KEY:
        kelkoo_names[1] = build_kelkoo_name_lookup_for_feed(FEED1_API_KEY, year, month)
        print(f"  Feed1: {len(kelkoo_names[1])} id -> name mappings")
    else:
        print("  Feed1: skipped (no FEED1_API_KEY)")
    if FEED2_API_KEY:
        kelkoo_names[2] = build_kelkoo_name_lookup_for_feed(FEED2_API_KEY, year, month)
        print(f"  Feed2: {len(kelkoo_names[2])} id -> name mappings")
    else:
        print("  Feed2: skipped (no FEED2_API_KEY)")
    print()
    build_logs(
        service,
        KELKOO_SHEETS_SPREADSHEET_ID,
        year,
        month,
        kelkoo_names_by_feed=kelkoo_names,
        dry_run=args.dry_run,
    )
    print("Done.")


if __name__ == "__main__":
    main()
