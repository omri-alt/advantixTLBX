#!/usr/bin/env python3
"""
Generate potential merchants list from traffic reports and write to the Blend spreadsheet.

Outputs are *feed-specific* sheets:
  - potentialKelkoo1 / potentialKelkoo2 — Kelkoo aggregated reports + merchants feed
  - potentialAdexa — Adexa ``GetShoppingSearchStats`` + GetMerchant URL + Link Monetizer
  - potentialYadore — Yadore ``/v2/conversion/detail/merchant`` + deeplink probe

Defaults:
  - shows BOTH monetized and unmonetized merchants (column `kelkoo_monetization` — same name for all feeds)
  - conversion-rate column is `cr` as a percent string (e.g. "1.23%")
  - Kelkoo thresholds: static CR >= 0.3%, flex CR >= 1.0%; Adexa/Yadore use flex threshold (1.0%).

Usage:
  python blend_potential_merchants.py --feed kelkoo1
  python blend_potential_merchants.py --feed adexa
  python blend_potential_merchants.py --feed yadore
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv

load_dotenv()

from config import (
    BLEND_SHEETS_SPREADSHEET_ID,
    FEED1_API_KEY,
    FEED2_API_KEY,
    FEED2_MERCHANTS_GEOS,
    YADORE_REPORT_DETAIL_MARKETS,
)
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
    return {
        "kelkoo1": "potentialKelkoo1",
        "kelkoo2": "potentialKelkoo2",
        "adexa": "potentialAdexa",
        "yadore": "potentialYadore",
    }.get(f, f"potential{f.title()}")


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


def _adexa_stat_mid(row: Dict[str, Any]) -> str:
    return str(
        row.get("merchantId")
        or row.get("merchant_id")
        or row.get("vendorMerchantId")
        or row.get("id")
        or ""
    ).strip()


def _adexa_stat_geo_lower(row: Dict[str, Any]) -> str:
    g = str(row.get("country") or row.get("geo") or row.get("market") or "").strip().lower()[:2]
    if g == "gb":
        return "uk"
    return g


def _adexa_stat_leads(row: Dict[str, Any]) -> int:
    for k in ("clicks", "clickCount", "searches", "searchCount", "leads", "leadCount", "impressions"):
        v = row.get(k)
        if v is None:
            continue
        try:
            return max(0, int(float(v)))
        except (TypeError, ValueError):
            continue
    return 0


def _adexa_stat_sales(row: Dict[str, Any]) -> int:
    for k in ("sales", "saleCount", "orders", "conversions", "conversionCount"):
        v = row.get(k)
        if v is None:
            continue
        try:
            return max(0, int(float(v)))
        except (TypeError, ValueError):
            continue
    return 0


def _adexa_stat_name(row: Dict[str, Any]) -> str:
    return str(row.get("merchantName") or row.get("merchant_name") or row.get("name") or "").strip()


def run_potential_adexa(
    service,
    out_sheet: str,
    start: str,
    end: str,
    *,
    only_monetized: bool,
) -> None:
    from integrations.adexa import AdexaClientError, fetch_shopping_search_stats, get_merchants, links_merchant_check

    try:
        raw_stats = fetch_shopping_search_stats(start, end)
    except AdexaClientError as e:
        raise RuntimeError(f"Adexa GetShoppingSearchStats: {e}") from e

    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in raw_stats:
        mid = _adexa_stat_mid(row)
        geo = _adexa_stat_geo_lower(row)
        if not mid or len(geo) != 2:
            continue
        key = (geo, mid)
        if key not in agg:
            agg[key] = {"leads": 0, "sales": 0, "name": _adexa_stat_name(row)}
        agg[key]["leads"] += _adexa_stat_leads(row)
        agg[key]["sales"] += _adexa_stat_sales(row)
        if not agg[key]["name"]:
            agg[key]["name"] = _adexa_stat_name(row)

    url_by_geo_mid: Dict[Tuple[str, str], str] = {}
    geos_needed = sorted({k[0] for k in agg})
    for geo in geos_needed:
        try:
            merchants = get_merchants(geo)
        except AdexaClientError:
            merchants = []
        for m in merchants:
            if not isinstance(m, dict):
                continue
            mid = str(m.get("id") or m.get("merchantId") or "").strip()
            if not mid:
                continue
            url = str(m.get("url") or m.get("merchantUrl") or m.get("website") or "").strip()
            url_by_geo_mid[(geo, mid)] = url

    min_cr = 0.01
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
    for (geo, mid), rec in sorted(agg.items(), key=lambda kv: kv[0]):
        leads = int(rec["leads"])
        sales = int(rec["sales"])
        cr = sales / max(leads, 1)
        if cr < min_cr:
            continue
        name = (rec.get("name") or "").strip() or mid
        domain = url_by_geo_mid.get((geo, mid), "").strip()
        tier = "Flex"
        if not domain:
            monetization = "no_merchant_url"
        else:
            checked += 1
            url_norm = domain if domain.lower().startswith("http") else f"https://{domain.lstrip('/')}"
            try:
                res = links_merchant_check(url_norm, geo)
                monetization = "monetized_adexa" if res.get("found") else f"not_monetized_adexa:{res.get('note', '')}"
            except AdexaClientError as e:
                monetization = f"not_monetized_adexa:{e}"
        is_monetized = monetization.startswith("monetized")
        if only_monetized and not is_monetized:
            continue
        rows_out.append(
            [
                mid,
                name,
                domain,
                geo,
                str(leads),
                str(sales),
                _cr_percent_str(sales, leads),
                tier,
                monetization,
            ]
        )

    def sort_key(r: List[str]) -> Tuple[float, int, int]:
        cr_num = float(r[6].replace("%", "")) if len(r) > 6 and str(r[6]).endswith("%") else 0.0
        return (-cr_num, -int(r[5]), -int(r[4]))

    rows_out.sort(key=sort_key)
    out = [header] + rows_out
    write_sheet(service, out_sheet, out)
    print(
        f"Wrote {len(rows_out)} rows to {out_sheet!r} (Adexa). Checked={checked}. only_monetized={only_monetized}"
    )


def run_potential_yadore(
    service,
    out_sheet: str,
    start: str,
    end: str,
    *,
    only_monetized: bool,
) -> None:
    from integrations.yadore import (
        YadoreClientError,
        deeplink,
        fetch_conversion_detail_merchant,
        parse_conversion_detail_merchant_rows,
    )

    markets = [str(m).strip().lower()[:2] for m in (YADORE_REPORT_DETAIL_MARKETS or []) if str(m).strip()]
    merged: List[Dict[str, Any]] = []
    try:
        if markets:
            for mkt in markets:
                payload = fetch_conversion_detail_merchant(start, end, market=mkt)
                merged.extend(parse_conversion_detail_merchant_rows(payload))
        else:
            payload = fetch_conversion_detail_merchant(start, end)
            merged.extend(parse_conversion_detail_merchant_rows(payload))
    except YadoreClientError as e:
        raise RuntimeError(f"Yadore conversion/detail/merchant: {e}") from e

    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in merged:
        mkt = str(row.get("market") or "").strip().lower()[:2]
        mid = str(row.get("merchant_id") or "").strip()
        if not mid or len(mkt) != 2:
            continue
        key = (mkt, mid)
        if key not in agg:
            agg[key] = {
                "leads": 0,
                "sales": 0,
                "name": str(row.get("merchant_name") or "").strip(),
                "url": str(row.get("merchant_url") or "").strip(),
            }
        agg[key]["leads"] += int(row.get("clicks") or 0)
        agg[key]["sales"] += int(row.get("sales") or 0)
        if not agg[key]["name"]:
            agg[key]["name"] = str(row.get("merchant_name") or "").strip()
        if not agg[key]["url"]:
            agg[key]["url"] = str(row.get("merchant_url") or "").strip()

    min_cr = 0.01
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
    for (geo, mid), rec in sorted(agg.items(), key=lambda kv: kv[0]):
        leads = int(rec["leads"])
        sales = int(rec["sales"])
        cr = sales / max(leads, 1)
        if cr < min_cr:
            continue
        name = (rec.get("name") or "").strip() or mid
        domain = (rec.get("url") or "").strip()
        tier = "Flex"
        if not domain:
            monetization = "no_merchant_url"
        else:
            checked += 1
            url_norm = domain if domain.lower().startswith("http") else f"https://{domain.lstrip('/')}"
            try:
                d = deeplink(url_norm, geo)
                monetization = "monetized_yadore" if d.get("found") else "not_monetized_yadore"
            except YadoreClientError as e:
                monetization = f"not_monetized_yadore:{e}"
        is_monetized = monetization.startswith("monetized")
        if only_monetized and not is_monetized:
            continue
        rows_out.append(
            [
                mid,
                name,
                domain,
                geo,
                str(leads),
                str(sales),
                _cr_percent_str(sales, leads),
                tier,
                monetization,
            ]
        )

    def sort_key_y(r: List[str]) -> Tuple[float, int, int]:
        cr_num = float(r[6].replace("%", "")) if len(r) > 6 and str(r[6]).endswith("%") else 0.0
        return (-cr_num, -int(r[5]), -int(r[4]))

    rows_out.sort(key=sort_key_y)
    out = [header] + rows_out
    write_sheet(service, out_sheet, out)
    print(
        f"Wrote {len(rows_out)} rows to {out_sheet!r} (Yadore). Checked={checked}. only_monetized={only_monetized}"
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--feed",
        required=True,
        choices=["kelkoo1", "kelkoo2", "adexa", "yadore"],
    )
    p.add_argument("--output", default=None, help="Output sheet name (default: potentialKelkoo1/2)")
    p.add_argument("--start", default=None)
    p.add_argument("--end", default=None)
    p.add_argument("--only-monetized", action="store_true", help="Hide unmonetized rows")
    args = p.parse_args()

    start, end = (args.start, args.end)
    if not start or not end:
        start, end = _month_to_yesterday_range()

    out_sheet = args.output or _default_output_sheet(args.feed)
    only_monetized = bool(args.only_monetized)

    service = get_sheets_service()

    if args.feed in ("adexa", "yadore"):
        print(f"Blend potential ({args.feed}): {start} -> {end}")
        if args.feed == "adexa":
            run_potential_adexa(service, out_sheet, start, end, only_monetized=only_monetized)
        else:
            run_potential_yadore(service, out_sheet, start, end, only_monetized=only_monetized)
        return

    api_key = _api_key_for_feed(args.feed)
    if not api_key:
        print(f"Error: API key missing for {args.feed}", file=sys.stderr)
        sys.exit(1)

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

    geo_list = list(FEED2_MERCHANTS_GEOS) if args.feed == "kelkoo2" and FEED2_MERCHANTS_GEOS else None
    merchants_feed = download_merchants_feed(api_key, geo_list, static_only=False)
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

    write_sheet(service, out_sheet, out)
    print(f"Wrote {len(rows_out)} rows to {out_sheet!r}. Checked={checked}. only_monetized={only_monetized}")


if __name__ == "__main__":
    main()

