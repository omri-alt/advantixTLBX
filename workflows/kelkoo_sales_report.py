"""
Yesterday Kelkoo raw-report sales export → Google Sheet tabs on the late-sales workbook.

Used by ``run_daily_workflow.py`` before Kelkoo late-sales detection. One tab per feed
(``SalesReport_feed{n}_{sale_day}_generated-{gen_day}``) when multiple feeds are configured;
a single feed keeps the legacy name ``SalesReport_{sale_day}_generated-{gen_day}``.
"""
from __future__ import annotations

import csv
import logging
import re
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from typing import Any, Dict, List, Sequence

import requests

from config import (
    DAILY_CONVERSION_POSTBACK_SALE_STATUS,
    KELKOO_LATE_SALES_SPREADSHEET_ID,
    discover_kelkoo_feed_api_keys,
    raw_report_geos_for_feed_index,
)
from integrations.daily_conversion_postbacks import (
    KELKOO_RAW_REPORT_URL,
    build_daily_postback_url,
)
from kelkoo_late_sales import sheet_title_a1_range

logger = logging.getLogger(__name__)
_DAILY_TAB_RE = re.compile(
    r"^SalesReport_(?:(feed\d+)_)?(\d{4}-\d{2}-\d{2})_generated-(\d{4}-\d{2}-\d{2})$",
    re.I,
)


def _utc_yesterday_iso() -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()


def _utc_today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _fetch_raw_tsv_yesterday(session: requests.Session, api_key: str, geo: str) -> tuple[int, str]:
    url = f"{KELKOO_RAW_REPORT_URL}?country={geo.lower()}&dateRange=yesterday"
    headers = {"Authorization": f"Bearer {api_key}", "Accept": "text/plain, */*"}
    r = session.get(url, headers=headers, timeout=120.0)
    return int(r.status_code), r.text or ""


def _click_id_from_raw_row(r: Dict[str, str], feed_index: int) -> str:
    """Kelkoo2 raw rows use ``custom1`` for Keitaro subid; feed1 uses ``publisherClickId``."""
    if feed_index == 2:
        for key in ("custom1", "Custom1"):
            v = r.get(key)
            if v is not None and str(v).strip():
                return str(v).strip()
    for key in ("publisherClickId", "PublisherClickId"):
        v = r.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _sale_rows_from_tsv(tsv: str, feed_index: int) -> List[Dict[str, Any]]:
    """Rows with ``sale`` true and ``leadValid`` true (aligned with daily postback parsing)."""
    reader = csv.DictReader(StringIO(tsv), delimiter="\t")
    out: List[Dict[str, Any]] = []
    for row in reader:
        if not isinstance(row, dict):
            continue
        r = {str(k): ("" if v is None else str(v)) for k, v in row.items()}
        if (r.get("sale") or "").lower() != "true":
            continue
        if (r.get("leadValid") or "").lower() != "true":
            continue
        click_id = _click_id_from_raw_row(r, feed_index)
        if not click_id:
            continue
        sale_value = (r.get("saleValueInUsd") or "0").strip() or "0"
        cpc = (r.get("leadEstimatedRevenueInUsd") or "0").strip() or "0"
        merchant = (r.get("merchantName") or r.get("MerchantName") or "").strip()
        country = (r.get("country") or r.get("Country") or "").strip()
        dt_raw = (r.get("dateTime") or r.get("DateTime") or "")[:10]
        try:
            pb = build_daily_postback_url(
                subid=click_id,
                payout=str(sale_value),
                status=DAILY_CONVERSION_POSTBACK_SALE_STATUS,
            )
        except Exception:
            pb = ""
        out.append(
            {
                "merchant": merchant,
                "date": dt_raw,
                "click_id": click_id,
                "lead_valid": (r.get("leadValid") or "").strip(),
                "sale": (r.get("sale") or "").strip(),
                "sale_value_usd": sale_value,
                "cpc": cpc,
                "country": country,
                "postback": pb,
            }
        )
    return out


def _write_sales_tab(
    service: Any,
    spreadsheet_id: str,
    title: str,
    dict_rows: Sequence[Dict[str, Any]],
) -> None:
    headers = [
        "merchant",
        "date",
        "click_id",
        "lead_valid",
        "sale",
        "sale_value_usd",
        "cpc",
        "country",
        "postback",
    ]
    data = [headers] + [[str(r.get(h, "") or "") for h in headers] for r in dict_rows]

    meta = service.get(spreadsheetId=spreadsheet_id, fields="sheets(properties(title))").execute()
    titles = [s.get("properties", {}).get("title") for s in meta.get("sheets", [])]
    if title not in titles:
        service.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
        ).execute()

    quoted = title.replace("'", "''")
    try:
        service.values().clear(spreadsheetId=spreadsheet_id, range=f"'{quoted}'!A1:Z50000").execute()
    except Exception:
        pass
    service.values().update(
        spreadsheetId=spreadsheet_id,
        range=sheet_title_a1_range(title, "A:I"),
        valueInputOption="USER_ENTERED",
        body={"values": data},
    ).execute()


def _build_7day_rows_from_daily_tabs(
    service: Any,
    spreadsheet_id: str,
    *,
    gen_day: date,
) -> List[Dict[str, Any]]:
    """
    Build one 7-day snapshot from existing daily ``SalesReport_*_generated-*`` tabs.

    Window: sale day in [gen_day-8, gen_day-2] inclusive (7 days), matching late-sales
    sale-date logic around the generation date.
    """
    lo = gen_day - timedelta(days=8)
    hi = gen_day - timedelta(days=2)
    meta = service.get(spreadsheetId=spreadsheet_id, fields="sheets(properties(title))").execute()
    selected: List[tuple[date, str]] = []
    for s in meta.get("sheets") or []:
        title = str((s.get("properties") or {}).get("title") or "")
        m = _DAILY_TAB_RE.match(title.strip())
        if not m:
            continue
        sale_s = m.group(2)
        try:
            sale_d = datetime.strptime(sale_s, "%Y-%m-%d").date()
        except ValueError:
            continue
        if lo <= sale_d <= hi:
            selected.append((sale_d, title))

    # Prefer newer sale-day tabs when duplicate click_id appears.
    selected.sort(key=lambda x: (x[0], x[1]), reverse=True)
    if not selected:
        return []

    ranges = [sheet_title_a1_range(t, "A:I") for _, t in selected]
    body = service.values().batchGet(spreadsheetId=spreadsheet_id, ranges=ranges).execute()

    headers = [
        "merchant",
        "date",
        "click_id",
        "lead_valid",
        "sale",
        "sale_value_usd",
        "cpc",
        "country",
        "postback",
    ]
    by_click: Dict[str, Dict[str, Any]] = {}
    for vr in body.get("valueRanges") or []:
        vals = vr.get("values") or []
        if not vals:
            continue
        hdr = [str(c or "").strip().lower() for c in vals[0]]
        if "click_id" not in hdr:
            continue
        idx = {name: (hdr.index(name) if name in hdr else None) for name in headers}
        for row in vals[1:]:
            cid_i = idx.get("click_id")
            if cid_i is None or cid_i >= len(row):
                continue
            click_id = str(row[cid_i] or "").strip()
            if not click_id or click_id in by_click:
                continue
            item: Dict[str, Any] = {}
            for h in headers:
                ii = idx.get(h)
                item[h] = str(row[ii] or "").strip() if ii is not None and ii < len(row) else ""
            by_click[click_id] = item
    return list(by_click.values())


def build_or_update_7day_sales_report(
    service: Any,
    *,
    dry_run: bool = False,
) -> Dict[str, Any]:
    sid = (KELKOO_LATE_SALES_SPREADSHEET_ID or "").strip()
    if not sid:
        return {"ok": False, "error": "no_spreadsheet", "tab": "", "rows": 0}
    gen_day = datetime.now(timezone.utc).date()
    tab = f"SalesReport_7days-generated-{gen_day.isoformat()}"
    rows = _build_7day_rows_from_daily_tabs(service, sid, gen_day=gen_day)
    if dry_run:
        print(f"   [dry-run] Would write {len(rows)} rows to {tab!r} (7-day snapshot).")
        return {"ok": True, "tab": tab, "rows": len(rows), "dry_run": True}
    _write_sales_tab(service, sid, tab, rows)
    print(f"   7-day snapshot: {len(rows)} rows → tab {tab!r}")
    return {"ok": True, "tab": tab, "rows": len(rows), "dry_run": False}


def run_yesterday_sales_reports(
    service: Any,
    *,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    For each configured Kelkoo feed: all geos (per-feed ``FEEDn_RAW_REPORT_GEOS`` or global list),
    fetch yesterday raw TSV, keep sale rows, write one sheet tab on ``KELKOO_LATE_SALES_SPREADSHEET_ID``.

    Returns a small summary dict for logging.
    """
    feeds = discover_kelkoo_feed_api_keys()
    if not feeds:
        msg = "No Kelkoo API keys (FEEDn_API_KEY or KEY_KL); skipping sales report."
        logger.warning(msg)
        print(f"   {msg}")
        return {"ok": False, "error": "no_api_keys", "tabs": []}

    sid = (KELKOO_LATE_SALES_SPREADSHEET_ID or "").strip()
    if not sid:
        msg = "KELKOO_LATE_SALES_SPREADSHEET_ID is empty; skipping sales report."
        logger.warning(msg)
        print(f"   {msg}")
        return {"ok": False, "error": "no_spreadsheet", "tabs": []}

    sale_day = _utc_yesterday_iso()
    gen_day = _utc_today_iso()
    multi = len(feeds) > 1
    session = requests.Session()
    tabs: List[str] = []
    summaries: List[Dict[str, Any]] = []

    for feed_index, api_key in feeds:
        geos = list(raw_report_geos_for_feed_index(feed_index))
        if not geos:
            logger.warning("Feed %s: no raw-report geos configured; skip.", feed_index)
            continue

        tab = (
            f"SalesReport_feed{feed_index}_{sale_day}_generated-{gen_day}"
            if multi
            else f"SalesReport_{sale_day}_generated-{gen_day}"
        )
        merged: List[Dict[str, Any]] = []
        errors: List[str] = []

        for geo in geos:
            try:
                status, body = _fetch_raw_tsv_yesterday(session, api_key, geo)
                if status != 200:
                    errors.append(f"{geo}:HTTP{status}")
                    logger.warning("Feed %s raw report %s: HTTP %s", feed_index, geo, status)
                    continue
                merged.extend(_sale_rows_from_tsv(body, feed_index))
            except Exception as e:
                errors.append(f"{geo}:{e}")
                logger.warning("Feed %s raw report %s: %s", feed_index, geo, e)

        summaries.append(
            {
                "feed_index": feed_index,
                "tab": tab,
                "rows": len(merged),
                "geos": len(geos),
                "errors": errors,
            }
        )

        print(
            f"   Sales report feed{feed_index}: {len(merged)} sale rows from {len(geos)} geos "
            f"→ tab {tab!r}"
            + (f" (warnings: {', '.join(errors[:6])}{'…' if len(errors) > 6 else ''})" if errors else "")
        )

        if dry_run:
            print(f"   [dry-run] Would write {len(merged)} rows to {tab!r} (sheet update skipped).")
            tabs.append(tab)
            continue

        try:
            _write_sales_tab(service, sid, tab, merged)
            tabs.append(tab)
        except Exception as e:
            logger.exception("Failed writing sales tab %s", tab)
            print(f"   ERROR writing tab {tab!r}: {e}")

    snapshot = build_or_update_7day_sales_report(service, dry_run=dry_run)
    return {
        "ok": True,
        "sale_day": sale_day,
        "gen_day": gen_day,
        "tabs": tabs,
        "feeds": summaries,
        "seven_day": snapshot,
    }
