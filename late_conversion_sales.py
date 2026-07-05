"""
Month-to-date late conversion sales: pull sales from all feeds, compare to Keitaro log, send LateSale GETs.

Sale window (UTC): first day of current month through day-before-yesterday. Yesterday stays on daily
``SaleOur`` postbacks. Keitaro dedup: skip when ``sub_id`` already has ``SaleOur`` or ``LateSale`` with
the same ``params.payout`` (repeat purchases on one click may have different payouts).

Sheets: one tab per feed per month on ``KELKOO_LATE_SALES_SPREADSHEET_ID``, refreshed daily in place
(``SalesMTD_{feed}_{YYYY-MM}``). Legacy ``SalesReport_*`` / ``*_late_sales_log`` tabs are pruned.
"""
from __future__ import annotations

import csv
import logging
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlencode, urlsplit, urlunsplit

import requests

logger = logging.getLogger(__name__)

MTD_TAB_RE = re.compile(r"^SalesMTD_([a-z0-9]+)_(\d{4}-\d{2})$", re.I)
_SALES_TAB_CONFLICT_SUFFIX_RE = re.compile(r"_conflict\d+$", re.I)

MTD_SHEET_HEADERS = [
    "sub_id",
    "sale_date",
    "merchant",
    "sale_value_usd",
    "country",
    "geo",
    "feed",
    "updated_utc",
]

POSTBACK_REQUEST_DELAY_SEC = 0.25
POSTBACK_REQUEST_TIMEOUT_SEC = 45


@dataclass(frozen=True)
class SaleRow:
    feed: str
    sub_id: str
    sale_date: str
    sale_value_usd: str
    merchant: str
    country: str
    geo: str = ""

    @property
    def dedup_key(self) -> Tuple[str, str]:
        from integrations.keitaro_conversions import normalize_payout

        return (self.sub_id, normalize_payout(self.sale_value_usd))


@dataclass
class LateSaleDiffRow:
    feed: str
    click_id: str
    date: str
    merchant: str
    sale_value_usd: str
    country: str
    postback_url: str
    skip_reason: str = ""


def sheet_title_a1_range(title: str, cell_range: str = "A:I") -> str:
    quoted = (title or "").replace("'", "''")
    return f"'{quoted}'!{cell_range}"


def late_sale_date_window(
    today: Optional[date] = None,
) -> Tuple[date, date, date, str]:
    """
    Returns ``(month_start, hi_date, yesterday, month_key)``.

    ``hi_date`` = day before yesterday (last day included in late scan).
    """
    today = today or datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    hi_date = today - timedelta(days=2)
    month_start = today.replace(day=1)
    month_key = f"{today.year:04d}-{today.month:02d}"
    return month_start, hi_date, yesterday, month_key


def effinity_sale_date_range(
    today: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Tuple[date, date, str]:
    """
    Effinity fetch + Keitaro dedup window: calendar month of ``end`` through ``end`` (default yesterday).

    On the 1st of a month, ``late_sale_date_window``'s ``month_start`` can be after ``yesterday``;
    this uses ``min(month_start, end.replace(day=1))`` so May 31 is still included on June 1.
    """
    month_start, _, yesterday, month_key = late_sale_date_window(today)
    end = end_date or yesterday
    range_start = min(month_start, end.replace(day=1))
    return range_start, end, month_key


def mtd_tab_title(feed: str, month_key: str) -> str:
    return f"SalesMTD_{(feed or '').strip().lower()}_{month_key}"


def build_postback_url(*, postback_base: str, click_id: str, sale_value_usd: str) -> str:
    base = (postback_base or "").strip().rstrip("/")
    if not base:
        raise ValueError("LATE_SALES_POSTBACK_BASE is empty")
    q = urlencode(
        {
            "subid": click_id,
            "payout": sale_value_usd,
            "status": "LateSale",
        }
    )
    parts = urlsplit(base)
    if "?" in (parts.path or ""):
        raise ValueError("postback base should not include query string")
    return urlunsplit((parts.scheme, parts.netloc, parts.path, q, parts.fragment))


def send_postback_gets(urls: List[str]) -> List[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for u in urls:
        err: str | None = None
        code: int | None = None
        try:
            r = requests.get(u, timeout=POSTBACK_REQUEST_TIMEOUT_SEC)
            code = r.status_code
            if code >= 400:
                err = (r.text or "")[:500]
        except requests.RequestException as e:
            err = str(e)
        results.append({"url": u, "http_status": code, "http_error": err})
        time.sleep(POSTBACK_REQUEST_DELAY_SEC)
    return results


def _configured_feed_tags() -> List[str]:
    from config import (
        ADEXA_API_KEY,
        ADEXA_SITE_ID,
        KELKOO_POSTBACK_FEED_TAGS,
        YADORE_API_KEY,
        kelkoo_api_key_for_postback_tag,
    )

    out: List[str] = []
    for tag in KELKOO_POSTBACK_FEED_TAGS:
        if kelkoo_api_key_for_postback_tag(tag):
            out.append(tag)
    if (ADEXA_SITE_ID or "").strip() and (ADEXA_API_KEY or "").strip():
        out.append("adexa")
    if (YADORE_API_KEY or "").strip():
        out.append("yadore")
    return out


def _sale_in_window(sale_date: str, month_start: date, hi_date: date) -> bool:
    s = (sale_date or "").strip()[:10]
    if len(s) < 10:
        return False
    try:
        d = datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return False
    return month_start <= d <= hi_date


def _parse_stat_date(stat: Dict[str, Any], fallback: str) -> str:
    for key in ("date", "day", "statDate", "saleDate", "conversionDate", "orderDate", "clickDate"):
        v = stat.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()[:10]
    return fallback[:10]


def fetch_kelkoo_mtd_sales(
    feed: str,
    *,
    month_start: date,
    hi_date: date,
    session: requests.Session,
) -> List[SaleRow]:
    from config import kelkoo_postback_tag_to_index, raw_report_geos_for_feed_index
    from config import kelkoo_api_key_for_postback_tag
    from integrations.daily_conversion_postbacks import KELKOO_RAW_REPORT_URL

    api_key = kelkoo_api_key_for_postback_tag(feed)
    if not api_key:
        return []
    feed_index = kelkoo_postback_tag_to_index(feed)
    geos = list(raw_report_geos_for_feed_index(feed_index))
    if not geos:
        return []

    start_s = month_start.isoformat()
    end_s = hi_date.isoformat()
    out: List[SaleRow] = []

    from workflows.kelkoo_sales_report import _sale_rows_from_tsv

    for geo in geos:
        url = f"{KELKOO_RAW_REPORT_URL}?country={geo.lower()}&start={start_s}&end={end_s}"
        headers = {"Authorization": f"Bearer {api_key}", "Accept": "text/plain, */*"}
        try:
            r = session.get(url, headers=headers, timeout=180.0)
            if r.status_code != 200:
                logger.warning("Kelkoo %s %s MTD raw HTTP %s", feed, geo, r.status_code)
                continue
            for row in _sale_rows_from_tsv(r.text or "", feed_index):
                sale_date = (row.get("date") or "")[:10]
                if not _sale_in_window(sale_date, month_start, hi_date):
                    continue
                cid = (row.get("click_id") or "").strip()
                if not cid:
                    continue
                out.append(
                    SaleRow(
                        feed=feed,
                        sub_id=cid,
                        sale_date=sale_date,
                        sale_value_usd=(row.get("sale_value_usd") or "0").strip() or "0",
                        merchant=(row.get("merchant") or "").strip(),
                        country=(row.get("country") or geo).strip(),
                        geo=geo.lower(),
                    )
                )
        except Exception as e:
            logger.warning("Kelkoo %s %s MTD fetch failed: %s", feed, geo, e)
    return out


def fetch_adexa_mtd_sales(
    *,
    month_start: date,
    hi_date: date,
) -> List[SaleRow]:
    from integrations.adexa import AdexaClientError, fetch_stats_raw
    from integrations.daily_conversion_postbacks import _adexa_stat_actions

    try:
        stats = fetch_stats_raw(month_start.isoformat(), hi_date.isoformat())
    except AdexaClientError as e:
        logger.warning("Adexa MTD StatsRaw failed: %s", e)
        return []

    out: List[SaleRow] = []
    for stat in stats:
        if not isinstance(stat, dict):
            continue
        parsed = _adexa_stat_actions(stat)
        if parsed is None:
            continue
        cid, _cpc, is_sale, sale_val = parsed
        if not is_sale:
            continue
        sale_date = _parse_stat_date(stat, hi_date.isoformat())
        if not _sale_in_window(sale_date, month_start, hi_date):
            continue
        out.append(
            SaleRow(
                feed="adexa",
                sub_id=cid,
                sale_date=sale_date,
                sale_value_usd=sale_val or "0",
                merchant=str(stat.get("merchantName") or stat.get("brand") or "").strip(),
                country=str(stat.get("country") or stat.get("geo") or "").strip()[:2].lower(),
            )
        )
    return out


def _yadore_mtd_markets() -> List[str]:
    """Markets for conversion/detail (configured list + account defaults)."""
    from integrations.yadore import yadore_conversion_detail_markets

    return yadore_conversion_detail_markets()


def fetch_yadore_mtd_sales(
    *,
    month_start: date,
    hi_date: date,
) -> List[SaleRow]:
    from integrations.daily_conversion_postbacks import _yadore_conversion_to_sale
    from integrations.yadore import (
        YadoreClientError,
        fetch_conversion_detail_clicks,
        fetch_conversion_general,
    )

    mkts = _yadore_mtd_markets()
    out: List[SaleRow] = []

    try:
        gen = fetch_conversion_general(month_start.isoformat(), hi_date.isoformat())
        total_sales = int((gen.get("total") or {}).get("sales") or 0) if isinstance(gen, dict) else 0
        logger.info(
            "Yadore conversion/general %s..%s: total sales=%s (API sanity check)",
            month_start,
            hi_date,
            total_sales,
        )
    except YadoreClientError as e:
        logger.warning("Yadore conversion/general failed (non-fatal): %s", e)

    d = month_start
    while d <= hi_date:
        day_s = d.isoformat()
        for mkt in mkts:
            try:
                conv_rows = fetch_conversion_detail_clicks(day_s, markets=[mkt])
            except YadoreClientError as e:
                logger.warning("Yadore conversion/detail %s %s failed: %s", day_s, mkt, e)
                continue

            sale_convs = [c for c in conv_rows if int(c.get("sales") or 0) > 0]
            if not sale_convs:
                continue

            for conv in sale_convs:
                parsed = _yadore_conversion_to_sale(conv)
                if parsed is None:
                    continue
                sub_id, payout, merchant, market = parsed
                sale_dt = (str(conv.get("date") or day_s) or day_s)[:10]
                if not _sale_in_window(sale_dt, month_start, hi_date):
                    continue
                out.append(
                    SaleRow(
                        feed="yadore",
                        sub_id=sub_id,
                        sale_date=sale_dt,
                        sale_value_usd=payout,
                        merchant=merchant,
                        country=market,
                        geo=market,
                    )
                )
        d += timedelta(days=1)

    logger.info("Yadore MTD conversion/detail: %s sale row(s) (%s markets)", len(out), len(mkts))
    return out


def fetch_all_mtd_sales(
    *,
    month_start: date,
    hi_date: date,
    feeds: Optional[Sequence[str]] = None,
    session: Optional[requests.Session] = None,
    progress: Optional[Any] = None,
) -> Dict[str, List[SaleRow]]:
    session = session or requests.Session()
    tags = list(feeds) if feeds else _configured_feed_tags()
    by_feed: Dict[str, List[SaleRow]] = {}
    for feed in tags:
        if progress is not None:
            try:
                progress(f"Fetching MTD sales: {feed} ...")
            except Exception:
                pass
        fl = feed.lower()
        if fl.startswith("kelkoo"):
            rows = fetch_kelkoo_mtd_sales(fl, month_start=month_start, hi_date=hi_date, session=session)
        elif fl == "adexa":
            rows = fetch_adexa_mtd_sales(month_start=month_start, hi_date=hi_date)
        elif fl == "yadore":
            rows = fetch_yadore_mtd_sales(month_start=month_start, hi_date=hi_date)
        else:
            rows = []
        by_feed[fl] = rows
        logger.info("Late conversion MTD fetch %s: %s sale row(s)", fl, len(rows))
    return by_feed


def _dedupe_sale_rows(rows: List[SaleRow]) -> List[SaleRow]:
    seen: Set[Tuple[str, str, str, str]] = set()
    out: List[SaleRow] = []
    for r in rows:
        key = (r.feed, r.sub_id, r.sale_date, r.dedup_key[1])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def write_mtd_sheet(
    service: Any,
    spreadsheet_id: str,
    tab: str,
    rows: Sequence[SaleRow],
    *,
    month_key: str,
) -> None:
    updated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    data_rows = [
        [
            r.sub_id,
            r.sale_date,
            r.merchant,
            r.sale_value_usd,
            r.country,
            r.geo,
            r.feed,
            updated,
        ]
        for r in rows
    ]
    values = [MTD_SHEET_HEADERS] + data_rows

    meta = service.get(spreadsheetId=spreadsheet_id, fields="sheets(properties(title))").execute()
    titles = [s.get("properties", {}).get("title") for s in meta.get("sheets", [])]
    if tab not in titles:
        service.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab}}}]},
        ).execute()

    quoted = tab.replace("'", "''")
    try:
        service.values().clear(spreadsheetId=spreadsheet_id, range=f"'{quoted}'!A1:Z500000").execute()
    except Exception:
        pass
    service.values().update(
        spreadsheetId=spreadsheet_id,
        range=sheet_title_a1_range(tab, "A:H"),
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()
    logger.info("Wrote %s rows to %s (month %s)", len(data_rows), tab, month_key)


def refresh_mtd_sales_sheets(
    service: Any,
    spreadsheet_id: str,
    *,
    dry_run: bool = False,
    feeds: Optional[Sequence[str]] = None,
    progress: Optional[Any] = None,
) -> Dict[str, Any]:
    month_start, hi_date, yesterday, month_key = late_sale_date_window()
    if hi_date < month_start:
        return {
            "ok": True,
            "month_key": month_key,
            "sale_window": f"{month_start} .. (empty — too early in month)",
            "feeds": {},
            "yesterday": yesterday.isoformat(),
        }

    by_feed = fetch_all_mtd_sales(month_start=month_start, hi_date=hi_date, feeds=feeds, progress=progress)
    feed_summaries: Dict[str, Any] = {}
    for feed, rows in by_feed.items():
        deduped = _dedupe_sale_rows(rows)
        tab = mtd_tab_title(feed, month_key)
        feed_summaries[feed] = {"tab": tab, "rows": len(deduped), "raw_rows": len(rows)}
        if dry_run:
            logger.info("[dry-run] Would write %s rows to %s", len(deduped), tab)
            continue
        write_mtd_sheet(service, spreadsheet_id, tab, deduped, month_key=month_key)

    return {
        "ok": True,
        "month_key": month_key,
        "sale_window": f"{month_start.isoformat()} .. {hi_date.isoformat()}",
        "yesterday": yesterday.isoformat(),
        "feeds": feed_summaries,
    }


def prune_legacy_sales_workbook_tabs(
    service: Any,
    spreadsheet_id: str,
    *,
    dry_run: bool = False,
    keep_month_key: Optional[str] = None,
) -> List[str]:
    """Remove legacy SalesReport / late_sales_log tabs and old SalesMTD months."""
    _, _, _, month_key = late_sale_date_window()
    keep_month_key = keep_month_key or month_key

    meta = service.get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))").execute()
    sheets = meta.get("sheets") or []
    if len(sheets) <= 1:
        return []

    to_delete: List[Tuple[int, str]] = []
    for s in sheets:
        props = s.get("properties") or {}
        title = str(props.get("title") or "")
        sheet_id = props.get("sheetId")
        if sheet_id is None or not title:
            continue
        base = _SALES_TAB_CONFLICT_SUFFIX_RE.sub("", title)

        m_mtd = MTD_TAB_RE.match(base)
        if m_mtd:
            if m_mtd.group(2) != keep_month_key:
                to_delete.append((int(sheet_id), title))
            continue

        if base.startswith("SalesReport_") or base.lower().endswith("_late_sales_log"):
            to_delete.append((int(sheet_id), title))
            continue

        if title != base and title.startswith("SalesReport_"):
            to_delete.append((int(sheet_id), title))

    if len(to_delete) >= len(sheets):
        keep = max(sheets, key=lambda x: int((x.get("properties") or {}).get("sheetId") or 0))
        keep_title = (keep.get("properties") or {}).get("title")
        to_delete = [(sid, t) for sid, t in to_delete if t != keep_title]

    deleted = [t for _sid, t in to_delete]
    if dry_run:
        logger.info("Tab prune [dry-run]: would delete %s tab(s)", len(deleted))
        return deleted

    for i in range(0, len(to_delete), 50):
        chunk = to_delete[i : i + 50]
        reqs = [{"deleteSheet": {"sheetId": sid}} for sid, _t in chunk]
        service.batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()

    if deleted:
        logger.info("Tab prune: deleted %s legacy/old MTD tab(s)", len(deleted))
    return deleted


def compute_eligible_late_sales(
    sales_by_feed: Dict[str, List[SaleRow]],
    *,
    keitaro_keys: Set[Tuple[str, str]],
    postback_base: str,
) -> Tuple[List[LateSaleDiffRow], int]:
    from integrations.keitaro_conversions import has_matching_sale_postback

    skipped_keitaro = 0
    eligible: List[LateSaleDiffRow] = []
    for feed, rows in sales_by_feed.items():
        for r in _dedupe_sale_rows(rows):
            if has_matching_sale_postback(r.sub_id, r.sale_value_usd, keitaro_keys):
                skipped_keitaro += 1
                eligible.append(
                    LateSaleDiffRow(
                        feed=feed,
                        click_id=r.sub_id,
                        date=r.sale_date,
                        merchant=r.merchant,
                        sale_value_usd=r.sale_value_usd,
                        country=r.country,
                        postback_url=build_postback_url(
                            postback_base=postback_base,
                            click_id=r.sub_id,
                            sale_value_usd=r.sale_value_usd,
                        ),
                        skip_reason="keitaro_sale_payout",
                    )
                )
                continue
            eligible.append(
                LateSaleDiffRow(
                    feed=feed,
                    click_id=r.sub_id,
                    date=r.sale_date,
                    merchant=r.merchant,
                    sale_value_usd=r.sale_value_usd,
                    country=r.country,
                    postback_url=build_postback_url(
                        postback_base=postback_base,
                        click_id=r.sub_id,
                        sale_value_usd=r.sale_value_usd,
                    ),
                    skip_reason="",
                )
            )
    return eligible, skipped_keitaro


def apply_yadore_saleour_backlog(
    *,
    dry_run: bool = False,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Send ``SaleOur`` postbacks (``payout=0``) for Yadore sales missing from Keitaro.

    Default range: month start through **yesterday** (UTC). Override with ``start_date`` /
    ``end_date`` (inclusive). Late-conversion window stops at day-before-yesterday; this
    catch-up includes yesterday for ``SaleOur`` daily alignment.
    """
    from config import DAILY_CONVERSION_POSTBACK_SALE_STATUS
    from integrations.daily_conversion_postbacks import build_daily_postback_url
    from integrations.keitaro_conversions import collect_sale_postback_keys, has_matching_sale_postback

    month_start, _hi_date, yesterday, month_key = late_sale_date_window()
    today = datetime.now(timezone.utc).date()
    start = start_date or month_start
    end = end_date or yesterday

    rows = _dedupe_sale_rows(fetch_yadore_mtd_sales(month_start=start, hi_date=end))
    try:
        keys = collect_sale_postback_keys(date_from=start, date_to=today)
    except Exception as e:
        return {"ok": False, "error": f"Keitaro log: {e}", "eligible": 0}

    to_send: List[SaleRow] = []
    skipped = 0
    for r in rows:
        if has_matching_sale_postback(r.sub_id, r.sale_value_usd, keys):
            skipped += 1
            continue
        to_send.append(r)

    urls = [
        build_daily_postback_url(
            subid=r.sub_id,
            payout="0",
            status=DAILY_CONVERSION_POSTBACK_SALE_STATUS,
        )
        for r in to_send
    ]

    postbacks_ok = 0
    postbacks_fail = 0
    failures: List[Dict[str, Any]] = []
    if not dry_run and urls:
        for r, u in zip(to_send, urls):
            pr = send_postback_gets([u])[0]
            code = pr.get("http_status")
            if code is not None and 200 <= int(code) < 400 and not pr.get("http_error"):
                postbacks_ok += 1
            else:
                postbacks_fail += 1
                failures.append(
                    {
                        "sub_id": r.sub_id,
                        "sale_date": r.sale_date,
                        "http_status": code,
                        "error": pr.get("http_error"),
                        "url": u,
                    }
                )

    return {
        "ok": dry_run or postbacks_fail == 0,
        "mode": "dry-run" if dry_run else "apply",
        "month_key": month_key,
        "sale_window": f"{start.isoformat()} .. {end.isoformat()}",
        "yadore_sales_found": len(rows),
        "skipped_keitaro": skipped,
        "eligible": len(to_send),
        "postbacks_ok": postbacks_ok if not dry_run else None,
        "postbacks_fail": postbacks_fail if not dry_run else None,
        "failures": failures if not dry_run else [],
        "sample_urls": urls[:5],
    }


def apply_yadore_saleour_missing_rows(
    missing: List[Dict[str, Any]],
    *,
    dry_run: bool = False,
    keitaro_from: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Send ``SaleOur`` for a precomputed missing list (e.g. from ``yadore_sales_keitaro_diff``).

    Re-checks Keitaro dedup before each send so a stale diff report does not double-fire.
    """
    from config import DAILY_CONVERSION_POSTBACK_SALE_STATUS
    from integrations.daily_conversion_postbacks import build_daily_postback_url
    from integrations.keitaro_conversions import (
        collect_sale_postback_keys,
        has_matching_sale_postback,
        normalize_payout,
    )

    today = datetime.now(timezone.utc).date()
    k_from = keitaro_from or today.replace(day=1)
    try:
        keys = collect_sale_postback_keys(date_from=k_from, date_to=today)
    except Exception as e:
        return {"ok": False, "error": f"Keitaro log: {e}", "eligible": 0}

    to_send: List[Dict[str, Any]] = []
    skipped = 0
    for row in missing:
        if not isinstance(row, dict):
            continue
        sid = str(row.get("sub_id") or "").strip()
        payout = str(row.get("payout") if row.get("payout") is not None else "0")
        if not sid:
            continue
        if has_matching_sale_postback(sid, payout, keys):
            skipped += 1
            continue
        to_send.append(row)

    sale_status = (DAILY_CONVERSION_POSTBACK_SALE_STATUS or "SaleOur").strip()
    urls = [
        build_daily_postback_url(
            subid=str(r["sub_id"]),
            payout=normalize_payout(str(r.get("payout") or "0")),
            status=sale_status,
        )
        for r in to_send
    ]

    postbacks_ok = 0
    postbacks_fail = 0
    failures: List[Dict[str, Any]] = []
    if not dry_run and urls:
        for r, u in zip(to_send, urls):
            pr = send_postback_gets([u])[0]
            code = pr.get("http_status")
            if code is not None and 200 <= int(code) < 400 and not pr.get("http_error"):
                postbacks_ok += 1
            else:
                postbacks_fail += 1
                failures.append(
                    {
                        "sub_id": r.get("sub_id"),
                        "sale_date": r.get("sale_date"),
                        "http_status": code,
                        "error": pr.get("http_error"),
                        "url": u,
                    }
                )

    return {
        "ok": dry_run or postbacks_fail == 0,
        "mode": "dry-run" if dry_run else "apply",
        "keitaro_dedup_from": k_from.isoformat(),
        "input_rows": len(missing),
        "skipped_keitaro": skipped,
        "eligible": len(to_send),
        "postbacks_ok": postbacks_ok if not dry_run else None,
        "postbacks_fail": postbacks_fail if not dry_run else None,
        "failures": failures if not dry_run else [],
        "sample_urls": urls[:5],
    }


def apply_effinity_mtd_cpasale_backlog(
    *,
    dry_run: bool = False,
    end_date: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Send ``salecpa`` postbacks (``payout=commissionAmount``) for Effinity sales missing from Keitaro.

    Default range: month of ``end`` (through yesterday) inclusive. Keitaro dedup uses ``(sub_id, payout)``
    for status ``salecpa`` (see ``EFFINITY_SALE_POSTBACK_STATUS``).
    """
    from config import EFFINITY_SALE_POSTBACK_STATUS
    from integrations.daily_conversion_postbacks import build_daily_postback_url
    from integrations.effinity import (
        EffinityClientError,
        _conversion_commission,
        _conversion_sub_id,
        fetch_mtd_sale_conversions,
    )
    from integrations.keitaro_conversions import collect_sale_postback_keys, has_matching_sale_postback

    range_start, end, month_key = effinity_sale_date_range(end_date=end_date)
    today = datetime.now(timezone.utc).date()

    try:
        raw_rows, api_err = fetch_mtd_sale_conversions(range_start, end, sales_only=True)
    except EffinityClientError as e:
        return {"ok": False, "error": str(e), "eligible": 0}
    if api_err and not raw_rows:
        return {"ok": False, "error": api_err, "eligible": 0}

    sale_status = (EFFINITY_SALE_POSTBACK_STATUS or "salecpa").strip()
    try:
        keys = collect_sale_postback_keys(
            date_from=range_start,
            date_to=today,
            statuses=[sale_status],
        )
    except Exception as e:
        return {"ok": False, "error": f"Keitaro log: {e}", "eligible": 0}

    to_send: List[Tuple[str, str, str]] = []
    skipped = 0
    for row in raw_rows:
        sid = _conversion_sub_id(row)
        payout = _conversion_commission(row)
        if has_matching_sale_postback(sid, payout, keys):
            skipped += 1
            continue
        conv_dt = str(row.get("conversionDate") or "")[:10]
        to_send.append((sid, payout, conv_dt))

    urls = [
        build_daily_postback_url(subid=sid, payout=payout, status=sale_status)
        for sid, payout, _ in to_send
    ]

    postbacks_ok = 0
    postbacks_fail = 0
    if not dry_run and urls:
        for u in urls:
            pr = send_postback_gets([u])[0]
            code = pr.get("http_status")
            if code is not None and 200 <= int(code) < 400 and not pr.get("http_error"):
                postbacks_ok += 1
            else:
                postbacks_fail += 1

    return {
        "ok": dry_run or postbacks_fail == 0,
        "mode": "dry-run" if dry_run else "apply",
        "source": "effinity",
        "month_key": month_key,
        "sale_window": f"{range_start.isoformat()} .. {end.isoformat()}",
        "effinity_sales_found": len(raw_rows),
        "skipped_keitaro": skipped,
        "eligible": len(to_send),
        "postbacks_ok": postbacks_ok if not dry_run else None,
        "postbacks_fail": postbacks_fail if not dry_run else None,
        "sample_urls": urls[:5],
        "api_warning": api_err,
    }


def run_late_sales_flow(
    *,
    credentials_path: Path,
    spreadsheet_id: str,
    postback_base: str,
    as_of_str: str = "",
    apply: bool,
    refresh_sheets: bool = True,
    prune_tabs: bool = True,
) -> dict[str, Any]:
    """
    Refresh MTD sales sheets, compare to Keitaro, optionally send LateSale GET postbacks.

    ``as_of_str`` is ignored (kept for API compatibility with the Flask UI).
    """
    _ = as_of_str
    mode = "apply" if apply else "dry-run"
    month_start, hi_date, yesterday, month_key = late_sale_date_window()

    if not credentials_path.is_file():
        return {"ok": False, "error": f"credentials.json not found at {credentials_path}", "mode": mode}

    if not (spreadsheet_id or "").strip():
        return {"ok": False, "error": "spreadsheet_id is empty", "mode": mode}

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        creds = service_account.Credentials.from_service_account_file(str(credentials_path))
        service = build("sheets", "v4", credentials=creds).spreadsheets()
        meta = service.get(spreadsheetId=spreadsheet_id, fields="properties(title)").execute()
        ss_title = (meta.get("properties") or {}).get("title") or ""
    except Exception as e:
        return {"ok": False, "error": str(e), "mode": mode}

    if hi_date < month_start:
        return {
            "ok": True,
            "mode": mode,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_title": ss_title,
            "month_key": month_key,
            "sale_window": f"{month_start} .. {hi_date} (no eligible days yet)",
            "yesterday": yesterday.isoformat(),
            "eligible_count": 0,
            "skipped_keitaro": 0,
            "rows": [],
            "feeds": {},
        }

    def _progress(msg: str) -> None:
        print(msg, flush=True)

    refresh_summary: Dict[str, Any] = {}
    if refresh_sheets:
        try:
            _progress(f"Refreshing SalesMTD_* sheets ({month_key}) ...")
            refresh_summary = refresh_mtd_sales_sheets(
                service, spreadsheet_id, dry_run=False, progress=_progress
            )
        except Exception as e:
            return {"ok": False, "error": f"MTD sheet refresh failed: {e}", "mode": mode}

    if prune_tabs:
        try:
            _progress("Pruning legacy SalesReport / old MTD tabs ...")
            prune_legacy_sales_workbook_tabs(service, spreadsheet_id, keep_month_key=month_key)
        except Exception as e:
            logger.warning("Tab prune failed (non-fatal): %s", e)

    _progress("Loading sales from network APIs (2nd pass for eligibility) ...")
    sales_by_feed = fetch_all_mtd_sales(
        month_start=month_start, hi_date=hi_date, progress=_progress
    )
    today = datetime.now(timezone.utc).date()
    try:
        from integrations.keitaro_conversions import collect_sale_postback_keys

        _progress(f"Loading Keitaro conversion log ({month_start} → {today}) …")
        keitaro_keys = collect_sale_postback_keys(date_from=month_start, date_to=today)
        logger.info(
            "Keitaro sale postback keys (month start → today): %s (sub_id+payout pairs)",
            len(keitaro_keys),
        )
    except Exception as e:
        logger.warning("Keitaro conversion log unavailable: %s", e)
        keitaro_keys = set()

    all_rows, skipped_keitaro = compute_eligible_late_sales(
        sales_by_feed, keitaro_keys=keitaro_keys, postback_base=postback_base
    )
    to_send = [r for r in all_rows if not r.skip_reason]

    postbacks_ok = 0
    postbacks_fail = 0
    result_rows: List[Dict[str, Any]] = []

    if apply and to_send:
        _progress(f"Sending {len(to_send)} LateSale GET postback(s) ...")
        urls = [r.postback_url for r in to_send]
        pb_results = send_postback_gets(urls)
        for r, pr in zip(to_send, pb_results):
            code = pr.get("http_status")
            err = pr.get("http_error")
            if code is not None and 200 <= code < 400 and not err:
                postbacks_ok += 1
            else:
                postbacks_fail += 1
            result_rows.append(
                {
                    "feed": r.feed,
                    "click_id": r.click_id,
                    "date": r.date,
                    "merchant": r.merchant,
                    "sale_value_usd": r.sale_value_usd,
                    "country": r.country,
                    "postback_url": r.postback_url,
                    "skip_reason": "",
                    "http_status": code,
                    "http_error": err,
                }
            )
        for r in all_rows:
            if r.skip_reason:
                result_rows.append(
                    {
                        "feed": r.feed,
                        "click_id": r.click_id,
                        "date": r.date,
                        "merchant": r.merchant,
                        "sale_value_usd": r.sale_value_usd,
                        "country": r.country,
                        "postback_url": r.postback_url,
                        "skip_reason": r.skip_reason,
                        "http_status": None,
                        "http_error": None,
                    }
                )
    else:
        for r in all_rows:
            result_rows.append(
                {
                    "feed": r.feed,
                    "click_id": r.click_id,
                    "date": r.date,
                    "merchant": r.merchant,
                    "sale_value_usd": r.sale_value_usd,
                    "country": r.country,
                    "postback_url": r.postback_url,
                    "skip_reason": r.skip_reason,
                    "http_status": None,
                    "http_error": None,
                }
            )

    if not apply:
        _progress(
            f"Done ({mode}): eligible={len(to_send)}, skipped_keitaro={skipped_keitaro}, "
            f"candidates={len(all_rows)}"
        )

    return {
        "ok": postbacks_fail == 0,
        "mode": mode,
        "spreadsheet_id": spreadsheet_id,
        "spreadsheet_title": ss_title,
        "month_key": month_key,
        "sale_window": f"{month_start.isoformat()} .. {hi_date.isoformat()}",
        "yesterday": yesterday.isoformat(),
        "feeds": refresh_summary.get("feeds") or {f: {"rows": len(rows)} for f, rows in sales_by_feed.items()},
        "keitaro_keys_loaded": len(keitaro_keys),
        "skipped_keitaro": skipped_keitaro,
        "eligible_count": len(to_send),
        "candidate_count": len(all_rows),
        "postbacks_ok": postbacks_ok if apply else None,
        "postbacks_fail": postbacks_fail if apply else None,
        "rows": sorted(result_rows, key=lambda x: (x.get("skip_reason") or "zzz", x.get("feed") or "", x.get("date") or "")),
    }
