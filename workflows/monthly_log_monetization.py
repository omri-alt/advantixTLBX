"""
Monthly merchant log: Kelkoo monetization column + URL resolution from merchants feed.

Shared by ``build_monthly_merchant_log.py``, ``enrich_monthly_log_monetization.py``,
and ``run_daily_workflow.py`` (yesterday snapshot before daily tabs are deleted).
"""
from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from config import FEED2_MERCHANTS_GEOS
from integrations.kelkoo_search import format_kelkoo_monetization_status, kelkoo_merchant_link_check
from workflows.kelkoo_daily import build_merchant_id_to_name_from_feed, download_merchants_feed

logger = logging.getLogger(__name__)

LOG_HEADERS_5 = [
    "Run date",
    "Country",
    "Merchant ID",
    "Merchant name",
    "Kelkoo monetization",
]

# Optional delay between Kelkoo link API calls (rate limits)
DEFAULT_REQUEST_DELAY_SEC = 0.15


def month_log_sheet_title(year: int, month: int, feed: int) -> str:
    name = date(year, month, 1).strftime("%B").lower()
    return f"{name}_log_{feed}"


def country_to_kelkoo_geo(country: str) -> str:
    """2-letter lowercase geo for Kelkoo API."""
    c = (country or "").strip().lower()
    if len(c) == 2:
        return c
    return c[:2] if len(c) > 2 else c


def build_merchant_geo_url_lookup(
    api_key: str,
    geos: Optional[List[str]] = None,
) -> Tuple[Dict[Tuple[str, str], str], Dict[str, str]]:
    """(geo, merchant_id) -> merchant url, and merchant_id -> url (any geo)."""
    merchants = download_merchants_feed(api_key, geos, static_only=False)
    by_geo_id: Dict[Tuple[str, str], str] = {}
    by_id: Dict[str, str] = {}
    for m in merchants:
        url = str(m.get("url") or "").strip()
        if not url:
            continue
        geo = str(m.get("geo_origin") or "").strip().lower()[:2]
        for key in (m.get("id"), m.get("websiteId")):
            if key is None:
                continue
            ks = str(key)
            if geo:
                by_geo_id[(geo, ks)] = url
            if ks not in by_id:
                by_id[ks] = url
    return by_geo_id, by_id


def resolve_merchant_url(
    country: str,
    merchant_id: str,
    by_geo_id: Dict[Tuple[str, str], str],
    by_id: Dict[str, str],
) -> str:
    g = country_to_kelkoo_geo(country)
    mid = str(merchant_id).strip()
    return by_geo_id.get((g, mid)) or by_id.get(mid, "")


def extract_unique_country_merchant_from_offers_values(values: List[List[Any]]) -> List[Tuple[str, str]]:
    """From offers sheet values: unique (country, merchant_id) preserving first-seen order."""
    if not values or len(values) < 2:
        return []
    headers = [str(h or "").strip().lower() for h in values[0]]

    def idx(*names: str) -> int:
        for n in names:
            if n in headers:
                return headers.index(n)
        return -1

    country_i = idx("country")
    mid_i = idx("merchant id", "merchant_id", "merchantid")
    if country_i < 0:
        country_i = 0
    if mid_i < 0:
        mid_i = 1

    seen: Set[Tuple[str, str]] = set()
    out: List[Tuple[str, str]] = []
    for row in values[1:]:
        if country_i >= len(row) or mid_i >= len(row):
            continue
        co = str(row[country_i] or "").strip().upper()
        mid = str(row[mid_i] or "").strip()
        if not co or not mid:
            continue
        key = (co, mid)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def monetization_status_for_merchant(
    country: str,
    merchant_id: str,
    api_key: str,
    by_geo_id: Dict[Tuple[str, str], str],
    by_id: Dict[str, str],
) -> str:
    url = resolve_merchant_url(country, merchant_id, by_geo_id, by_id)
    if not url:
        return "no_merchant_url"
    geo = country_to_kelkoo_geo(country)
    if len(geo) != 2:
        return "bad_geo"
    res = kelkoo_merchant_link_check(url, geo, api_key)
    return format_kelkoo_monetization_status(res)


def normalize_log_rows_to_five_cols(rows: List[List[Any]]) -> List[List[Any]]:
    """Ensure each row has 5 columns (pad/truncate)."""
    out: List[List[Any]] = []
    for row in rows:
        r = list(row) + [""] * 5
        out.append(r[:5])
    return out


def read_sheet_values_raw(
    service: Any,
    spreadsheet_id: str,
    sheet_name: str,
    range_a1: str = "A:Z",
) -> List[List[Any]]:
    quoted = sheet_name.replace("'", "''")
    try:
        return (
            service.values()
            .get(spreadsheetId=spreadsheet_id, range=f"'{quoted}'!{range_a1}")
            .execute()
            .get("values")
            or []
        )
    except Exception as e:
        logger.warning("read_sheet %s: %s", sheet_name, e)
        return []


def ensure_month_log_sheet_exists(service: Any, spreadsheet_id: str, sheet_name: str) -> None:
    meta = service.get(spreadsheetId=spreadsheet_id, fields="sheets(properties(title))").execute()
    titles = [s["properties"]["title"] for s in meta.get("sheets", [])]
    if sheet_name not in titles:
        service.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]},
        ).execute()


def write_full_log_sheet(service: Any, spreadsheet_id: str, sheet_name: str, rows: List[List[Any]]) -> None:
    ensure_month_log_sheet_exists(service, spreadsheet_id, sheet_name)
    quoted = sheet_name.replace("'", "''")
    service.values().clear(spreadsheetId=spreadsheet_id, range=f"'{quoted}'!A1:Z50000").execute()
    if rows:
        service.values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{quoted}'!A1",
            valueInputOption="USER_ENTERED",
            body={"values": rows},
        ).execute()


def enrich_log_rows_monetization(
    rows: List[List[Any]],
    api_key: str,
    by_geo_id: Dict[Tuple[str, str], str],
    by_id: Dict[str, str],
    *,
    only_run_date: Optional[str] = None,
    force: bool = False,
    delay_sec: float = DEFAULT_REQUEST_DELAY_SEC,
) -> Tuple[List[List[Any]], int]:
    """
    rows[0] = header (upgraded to 5 cols). Data rows: set column E (index 4) monetization.

    - If ``only_run_date`` is set (YYYY-MM-DD), only those rows are considered for API calls.
    - If ``force`` is False, skip rows that already have a non-empty column E.
    Returns (new_rows, api_calls_made).
    """
    if not rows:
        return [LOG_HEADERS_5.copy()], 0
    header = LOG_HEADERS_5.copy()
    body = normalize_log_rows_to_five_cols(rows[1:])
    calls = 0
    for row in body:
        run_d = str(row[0] if len(row) > 0 else "").strip()
        if only_run_date is not None and run_d != only_run_date:
            continue
        existing_e = str(row[4] if len(row) > 4 else "").strip()
        if not force and existing_e:
            continue
        country = str(row[1] if len(row) > 1 else "").strip()
        mid = str(row[2] if len(row) > 2 else "").strip()
        if not country or not mid:
            continue
        status = monetization_status_for_merchant(country, mid, api_key, by_geo_id, by_id)
        while len(row) < 5:
            row.append("")
        row[4] = status
        calls += 1
        if delay_sec > 0:
            time.sleep(delay_sec)
    return [header] + body, calls


def count_enrich_candidates(
    rows: List[List[Any]],
    *,
    only_run_date: Optional[str] = None,
    force: bool = False,
) -> int:
    """How many data rows would get a Kelkoo check (same rules as ``enrich_log_rows_monetization``)."""
    if not rows or len(rows) < 2:
        return 0
    body = normalize_log_rows_to_five_cols(rows[1:])
    n = 0
    for row in body:
        run_d = str(row[0] if len(row) > 0 else "").strip()
        if only_run_date is not None and run_d != only_run_date:
            continue
        existing_e = str(row[4] if len(row) > 4 else "").strip()
        if not force and existing_e:
            continue
        country = str(row[1] if len(row) > 1 else "").strip()
        mid = str(row[2] if len(row) > 2 else "").strip()
        if country and mid:
            n += 1
    return n


def upsert_yesterday_merchants_into_monthly_log(
    service: Any,
    spreadsheet_id: str,
    yesterday_str: str,
    feed: int,
    api_key: str,
    *,
    delay_sec: float = DEFAULT_REQUEST_DELAY_SEC,
) -> int:
    """
    Before daily sheets are deleted: read ``yesterday_str_offers_{feed}``, append/update
    rows in ``{month}_log_{feed}`` for ``Run date`` = yesterday_str, and set Kelkoo monetization (col E).

    Returns number of Kelkoo API calls made.
    """
    return upsert_run_merchants_into_monthly_log(
        service,
        spreadsheet_id,
        yesterday_str,
        feed,
        api_key=api_key,
        check_monetization=True,
        delay_sec=delay_sec,
    )


def upsert_run_merchants_into_monthly_log(
    service: Any,
    spreadsheet_id: str,
    run_date_str: str,
    feed: int,
    *,
    api_key: Optional[str],
    check_monetization: bool,
    delay_sec: float = DEFAULT_REQUEST_DELAY_SEC,
) -> int:
    """
    Upsert merchants from ``{run_date_str}_offers_{feed}`` into ``{month}_log_{feed}``.

    - Always writes merchant name (from the live merchants feed) when ``api_key`` is provided.
    - When ``check_monetization=True``: fills column E (Kelkoo monetization) via
      Kelkoo ``search/link`` for that run date.
    - When ``check_monetization=False``: leaves column E empty for new rows and does not
      wipe existing E values.
    """
    if not run_date_str or len(run_date_str) < 10:
        return 0
    if not api_key or not api_key.strip():
        # We can still upsert IDs without monetization, but names require the feed download.
        api_key = ""

    y, m = int(run_date_str[0:4]), int(run_date_str[5:7])
    log_name = month_log_sheet_title(y, m, feed)
    offers_name = f"{run_date_str}_offers_{feed}"

    offers_vals = read_sheet_values_raw(service, spreadsheet_id, offers_name)
    pairs = extract_unique_country_merchant_from_offers_values(offers_vals)
    if not pairs:
        logger.info("No offers data for %s; skip monthly log upsert", offers_name)
        return 0

    name_lookup: Dict[str, str] = {}
    by_geo_id: Dict[Tuple[str, str], str] = {}
    by_id: Dict[str, str] = {}
    merchants_geos: Optional[List[str]] = None
    if feed == 2 and FEED2_MERCHANTS_GEOS:
        merchants_geos = list(FEED2_MERCHANTS_GEOS)
    if api_key:
        name_lookup = build_merchant_id_to_name_from_feed(api_key, merchants_geos)
        if check_monetization:
            by_geo_id, by_id = build_merchant_geo_url_lookup(api_key, merchants_geos)

    raw = read_sheet_values_raw(service, spreadsheet_id, log_name, "A:Z")
    body: List[List[Any]] = normalize_log_rows_to_five_cols(raw[1:]) if raw else []

    # Index: (run_date, country, mid) -> row index in body (0-based)
    index: Dict[Tuple[str, str, str], int] = {}
    for i, row in enumerate(body):
        if len(row) < 3:
            continue
        rd = str(row[0] or "").strip()
        co = str(row[1] or "").strip().upper()
        mid = str(row[2] or "").strip()
        if rd and co and mid:
            index[(rd, co, mid)] = i

    api_calls = 0
    for country, mid in pairs:
        key = (run_date_str, country, mid)
        mname = name_lookup.get(mid, "")

        if check_monetization and api_key:
            url = resolve_merchant_url(country, mid, by_geo_id, by_id)
            if not url:
                status = "no_merchant_url"
            else:
                geo = country_to_kelkoo_geo(country)
                if len(geo) != 2:
                    status = "bad_geo"
                else:
                    res = kelkoo_merchant_link_check(url, geo, api_key)
                    status = format_kelkoo_monetization_status(res)
                    api_calls += 1
                    if delay_sec > 0:
                        time.sleep(delay_sec)
        else:
            status = ""

        if key in index:
            row = body[index[key]]
            while len(row) < 5:
                row.append("")
            # Fill name if missing
            if mname and not str(row[3] if len(row) > 3 else "").strip():
                row[3] = mname
            # Fill monetization only when explicitly requested
            if check_monetization and status:
                row[4] = status
        else:
            body.append([run_date_str, country, mid, mname, status])

    body.sort(key=lambda r: (str(r[0]), str(r[1]), str(r[2])))
    write_full_log_sheet(service, spreadsheet_id, log_name, [LOG_HEADERS_5] + body)
    return api_calls
