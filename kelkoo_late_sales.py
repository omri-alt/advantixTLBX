"""
Kelkoo late-sales (KLtools): diff last two ``SalesReport_7days-generated-*`` tabs,
apply sale-date window rules, build postback URLs, optionally GET each URL.

Dedup before sending (apply or dry-run display):
  - Keitaro conversion log (``POST .../conversions/log``): skip if ``sub_id`` already has status ``LateSale``.
  - ``{month}_late_sales_log`` tabs: ``click_id`` already has ``late_postback_fired_at_utc`` set.
  - Optional (``LATE_SALES_SKIP_IF_IN_DAILY_TAB=1``): skip if ``click_id`` is on a daily ``SalesReport_*`` tab.
    Default off — daily tabs list sales; on-time ``SaleOur`` is separate from ``LateSale``.

After successful LateSale GETs (apply), append rows to ``{month}_late_sales_log`` for the
**generation month** of the newer 7-day tab (``d_new``), e.g. ``april_late_sales_log``.

Used by ``tools/kelkoo_late_sales_7day_diff.py`` and the Flask UI.
"""
from __future__ import annotations

import calendar
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode, urlsplit, urlunsplit

import requests

logger = logging.getLogger(__name__)

TAB_RE = re.compile(r"^SalesReport_7days-generated-(\d{4}-\d{2}-\d{2})$")
DAILY_TAB_RE = re.compile(
    r"^SalesReport_(?:(feed\d+)_)?(\d{4}-\d{2}-\d{2})_generated-(\d{4}-\d{2}-\d{2})$",
    re.I,
)
_SALES_TAB_CONFLICT_SUFFIX_RE = re.compile(r"_conflict\d+$", re.I)

_MONTH_EN = (
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
)

_LATE_LOG_TAB_RE = re.compile(
    r"^(" + "|".join(_MONTH_EN) + r")_late_sales_log$",
    re.I,
)

LATE_SALES_LOG_HEADERS = [
    "click_id",
    "sale_date",
    "merchant",
    "sale_value_usd",
    "country",
    "postback_url",
    "late_postback_fired",
    "late_postback_fired_at_utc",
    "source",
]

POSTBACK_REQUEST_DELAY_SEC = 0.25
POSTBACK_REQUEST_TIMEOUT_SEC = 45
DAILY_BATCH_RANGES = 80


def _strip_sales_tab_conflict_suffix(title: str) -> str:
    return _SALES_TAB_CONFLICT_SUFFIX_RE.sub("", (title or "").strip())


def sales_tab_anchor_date(title: str) -> Optional[date]:
    """
    Date used for KLtools retention: 7-day tab generation day, or daily tab ``generated`` day.
    """
    base = _strip_sales_tab_conflict_suffix(title)
    m = TAB_RE.match(base)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            return None
    m = DAILY_TAB_RE.match(base)
    if m:
        try:
            return datetime.strptime(m.group(3), "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def _late_log_month_range(title: str, today: date) -> Optional[Tuple[date, date]]:
    m = _LATE_LOG_TAB_RE.match((title or "").strip())
    if not m:
        return None
    month_name = m.group(1).lower()
    try:
        mm = _MONTH_EN.index(month_name) + 1
    except ValueError:
        return None
    yy = today.year
    if mm > today.month:
        yy -= 1
    last_dom = calendar.monthrange(yy, mm)[1]
    return date(yy, mm, 1), date(yy, mm, last_dom)


def _late_log_overlaps_retention(title: str, cutoff: date, today: date) -> bool:
    """Keep monthly log tabs that cover at least one day on or after ``cutoff``."""
    rng = _late_log_month_range(title, today)
    if not rng:
        return True
    _start, month_end = rng
    return month_end >= cutoff


def prune_old_sales_workbook_tabs(
    service: Any,
    spreadsheet_id: str,
    *,
    retention_days: Optional[int] = None,
    dry_run: bool = False,
) -> List[str]:
    """
    Delete KLtools ``SalesReport_*`` tabs (incl. ``_conflict*`` dupes) older than retention.
    Drops ``{month}_late_sales_log`` tabs for months fully before the retention window.
    """
    from config import KELKOO_SALES_TAB_RETENTION_DAYS

    days = int(retention_days if retention_days is not None else KELKOO_SALES_TAB_RETENTION_DAYS)
    today = datetime.now(timezone.utc).date()
    cutoff = today - timedelta(days=max(1, days))

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

        anchor = sales_tab_anchor_date(title)
        if anchor is not None:
            if anchor < cutoff:
                to_delete.append((int(sheet_id), title))
            continue

        if _LATE_LOG_TAB_RE.match(title):
            if not _late_log_overlaps_retention(title, cutoff, today):
                to_delete.append((int(sheet_id), title))
            continue

        if _strip_sales_tab_conflict_suffix(title) != title and title.startswith("SalesReport_"):
            to_delete.append((int(sheet_id), title))

    if len(to_delete) >= len(sheets):
        keep_id = max(sheets, key=lambda x: int((x.get("properties") or {}).get("sheetId") or 0))
        keep_title = (keep_id.get("properties") or {}).get("title")
        to_delete = [(sid, t) for sid, t in to_delete if t != keep_title]

    deleted_titles = [t for _sid, t in to_delete]
    if dry_run:
        logger.info(
            "KLtools tab prune [dry-run]: would delete %s tab(s) older than %s (retention %sd)",
            len(deleted_titles),
            cutoff.isoformat(),
            days,
        )
        return deleted_titles

    for i in range(0, len(to_delete), 50):
        chunk = to_delete[i : i + 50]
        reqs = [{"deleteSheet": {"sheetId": sid}} for sid, _t in chunk]
        service.batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()

    if deleted_titles:
        logger.info(
            "KLtools tab prune: deleted %s tab(s) with anchor before %s (retention %sd)",
            len(deleted_titles),
            cutoff.isoformat(),
            days,
        )
    return deleted_titles


def sheet_title_a1_range(title: str, cell_range: str = "A:I") -> str:
    q = title.replace("'", "''")
    return f"'{q}'!{cell_range}"


def parse_gen_date_from_tab_title(title: str) -> date | None:
    m = TAB_RE.match(title.strip())
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y-%m-%d").date()


def parse_row_sale_date(raw: str) -> date | None:
    raw = (raw or "").strip()
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        try:
            return datetime.strptime(raw[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def late_sale_eligible_sale_date_range(report_gen_date: date) -> tuple[date, date]:
    """Inclusive sale-date window: ends R-2, spans 7 days (excludes R-1 on-time and R)."""
    end = report_gen_date - timedelta(days=2)
    start = end - timedelta(days=6)
    return start, end


def filter_rows_by_late_sale_window(
    header: list[str],
    rows: list[list[str]],
    report_gen_date: date,
) -> tuple[list[list[str]], int]:
    try:
        idx_date = header.index("date")
    except ValueError:
        return list(rows), 0
    lo, hi = late_sale_eligible_sale_date_range(report_gen_date)
    kept: list[list[str]] = []
    dropped = 0
    for r in rows:
        cell = str(r[idx_date] if idx_date < len(r) else "") or ""
        d = parse_row_sale_date(cell)
        if d is None or d < lo or d > hi:
            dropped += 1
            continue
        kept.append(r)
    return kept, dropped


def load_tab_rows(service: Any, spreadsheet_id: str, title: str) -> tuple[list[str], list[list[str]]]:
    rng = sheet_title_a1_range(title, "A:I")
    res = service.values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    rows = res.get("values") or []
    if not rows:
        return [], []
    header = [str(c or "").strip().lower() for c in rows[0]]
    if "click_id" not in header:
        raise ValueError(f"Tab {title!r}: missing click_id column; header={header[:20]}")
    return header, rows[1:]


def list_7day_tabs(meta: dict[str, Any]) -> list[tuple[date, str]]:
    dated: list[tuple[date, str]] = []
    for s in meta.get("sheets") or []:
        t = (s.get("properties") or {}).get("title") or ""
        d = parse_gen_date_from_tab_title(t)
        if d:
            dated.append((d, t))
    dated.sort(key=lambda x: x[0], reverse=True)
    return dated


def sheet_titles(meta: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for s in meta.get("sheets") or []:
        t = (s.get("properties") or {}).get("title")
        if t:
            out.add(str(t))
    return out


def pick_new_and_old_tab(
    dated_tabs: list[tuple[date, str]],
    as_of: date | None,
) -> tuple[date, str, date, str]:
    if len(dated_tabs) < 2:
        raise ValueError(
            f"Need at least two SalesReport_7days-generated-YYYY-MM-DD tabs; found {len(dated_tabs)}."
        )
    if as_of is not None:
        idx = next((i for i, (d, _) in enumerate(dated_tabs) if d == as_of), None)
        if idx is None:
            avail = [str(d) for d, _ in dated_tabs[:20]]
            raise ValueError(f"No 7-day tab for as_of={as_of}. Recent: {avail}")
        if idx + 1 >= len(dated_tabs):
            raise ValueError(f"No older 7-day tab to compare for as_of={as_of}.")
        d_new, tab_new = dated_tabs[idx]
        d_old, tab_old = dated_tabs[idx + 1]
        return d_new, tab_new, d_old, tab_old
    return dated_tabs[0][0], dated_tabs[0][1], dated_tabs[1][0], dated_tabs[1][1]


def header_index(header: list[str], name: str) -> int | None:
    try:
        return header.index(name.lower())
    except ValueError:
        return None


def row_get(header: list[str], row: list[str], col: str) -> str:
    i = header_index(header, col)
    if i is None or i >= len(row):
        return ""
    return str(row[i] or "").strip()


def month_late_log_sheet_title(d: date) -> str:
    return f"{_MONTH_EN[d.month - 1]}_late_sales_log"


def _late_log_tab_names_for_dedup(d_new: date, sale_dates: list[date | None]) -> list[str]:
    """Month tabs to scan for prior LateSale fires (this month, neighbors, and months of sale dates)."""
    ym: set[tuple[int, int]] = {(d_new.year, d_new.month)}
    for sd in sale_dates:
        if sd:
            ym.add((sd.year, sd.month))
    expanded: set[tuple[int, int]] = set(ym)
    for y, m in list(ym):
        if m == 1:
            expanded.add((y - 1, 12))
        else:
            expanded.add((y, m - 1))
        if m == 12:
            expanded.add((y + 1, 1))
        else:
            expanded.add((y, m + 1))
    names = [f"{_MONTH_EN[mm - 1]}_late_sales_log" for y, mm in sorted(expanded)]
    # preserve unique order
    seen: set[str] = set()
    out: list[str] = []
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out


def collect_daily_sale_click_ids(service: Any, spreadsheet_id: str, meta: dict[str, Any]) -> set[str]:
    titles = [t for t in sheet_titles(meta) if DAILY_TAB_RE.match(t)]
    if not titles:
        return set()
    out: set[str] = set()
    for i in range(0, len(titles), DAILY_BATCH_RANGES):
        chunk = titles[i : i + DAILY_BATCH_RANGES]
        ranges = [sheet_title_a1_range(t, "A:I") for t in chunk]
        body = service.values().batchGet(spreadsheetId=spreadsheet_id, ranges=ranges).execute()
        for vr in body.get("valueRanges") or []:
            vals = vr.get("values") or []
            if not vals:
                continue
            hdr = [str(c or "").strip().lower() for c in vals[0]]
            if "click_id" not in hdr:
                continue
            ic = hdr.index("click_id")
            for r in vals[1:]:
                if ic < len(r):
                    cid = str(r[ic] or "").strip()
                    if cid:
                        out.add(cid)
    return out


def _row_logged_fired(header: list[str], row: list[str]) -> bool:
    at = header_index(header, "late_postback_fired_at_utc")
    if at is not None and at < len(row) and str(row[at]).strip():
        return True
    fl = header_index(header, "late_postback_fired")
    if fl is not None and fl < len(row):
        v = str(row[fl]).strip().lower()
        if v in ("yes", "true", "1", "y", "x"):
            return True
    return False


def collect_logged_fired_click_ids(
    service: Any,
    spreadsheet_id: str,
    meta: dict[str, Any],
    d_new: date,
    sale_dates: list[date | None],
) -> set[str]:
    titles = sheet_titles(meta)
    fired: set[str] = set()
    for name in _late_log_tab_names_for_dedup(d_new, sale_dates):
        if name not in titles:
            continue
        try:
            res = service.values().get(spreadsheetId=spreadsheet_id, range=sheet_title_a1_range(name, "A:J")).execute()
        except Exception:
            continue
        vals = res.get("values") or []
        if not vals:
            continue
        hdr = [str(c or "").strip().lower() for c in vals[0]]
        ic = header_index(hdr, "click_id")
        if ic is None:
            continue
        for r in vals[1:]:
            if ic >= len(r):
                continue
            cid = str(r[ic] or "").strip()
            if not cid:
                continue
            if _row_logged_fired(hdr, r):
                fired.add(cid)
    return fired


def ensure_late_log_sheet_with_header(service: Any, spreadsheet_id: str, log_title: str, titles: set[str]) -> None:
    if log_title not in titles:
        service.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": log_title}}}]},
        ).execute()
        titles.add(log_title)
        service.values().update(
            spreadsheetId=spreadsheet_id,
            range=sheet_title_a1_range(log_title, "A1:I1"),
            valueInputOption="USER_ENTERED",
            body={"values": [LATE_SALES_LOG_HEADERS]},
        ).execute()
        return
    try:
        res = service.values().get(spreadsheetId=spreadsheet_id, range=sheet_title_a1_range(log_title, "A1:I1")).execute()
        vals = res.get("values") or []
        if not vals or str(vals[0][0] or "").strip().lower() != "click_id":
            service.values().update(
                spreadsheetId=spreadsheet_id,
                range=sheet_title_a1_range(log_title, "A1:I1"),
                valueInputOption="USER_ENTERED",
                body={"values": [LATE_SALES_LOG_HEADERS]},
            ).execute()
    except Exception:
        service.values().update(
            spreadsheetId=spreadsheet_id,
            range=sheet_title_a1_range(log_title, "A1:I1"),
            valueInputOption="USER_ENTERED",
            body={"values": [LATE_SALES_LOG_HEADERS]},
        ).execute()


def append_late_log_rows(service: Any, spreadsheet_id: str, log_title: str, rows: list[list[Any]]) -> None:
    if not rows:
        return
    service.values().append(
        spreadsheetId=spreadsheet_id,
        range=sheet_title_a1_range(log_title, "A:I"),
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


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
    path = parts.path or ""
    if "?" in path:
        raise ValueError("postback base should not include query string; use LATE_SALES_POSTBACK_BASE without ?params")
    return urlunsplit((parts.scheme, parts.netloc, parts.path, q, parts.fragment))


@dataclass
class LateSaleDiffRow:
    click_id: str
    date: str
    merchant: str
    sale_value_usd: str
    country: str
    postback_url: str


def _compute_new_late_sale_rows_inner(
    service: Any,
    spreadsheet_id: str,
    meta: dict[str, Any],
    as_of: date | None,
) -> dict[str, Any]:
    ss_title = (meta.get("properties") or {}).get("title") or ""
    dated_tabs = list_7day_tabs(meta)
    d_new, tab_new, d_old, tab_old = pick_new_and_old_tab(dated_tabs, as_of)

    header, rows_new = load_tab_rows(service, spreadsheet_id, tab_new)
    _, rows_old = load_tab_rows(service, spreadsheet_id, tab_old)

    rows_new_f, drop_new = filter_rows_by_late_sale_window(header, rows_new, d_new)
    rows_old_f, drop_old = filter_rows_by_late_sale_window(header, rows_old, d_old)

    idx_click = header.index("click_id")
    ids_old: set[str] = set()
    for r in rows_old_f:
        if idx_click < len(r):
            cid = str(r[idx_click] or "").strip()
            if cid:
                ids_old.add(cid)

    new_rows: list[list[str]] = []
    seen_new: set[str] = set()
    dup = 0
    for r in rows_new_f:
        if idx_click >= len(r):
            continue
        cid = str(r[idx_click] or "").strip()
        if not cid:
            continue
        if cid in seen_new:
            dup += 1
            continue
        seen_new.add(cid)
        if cid not in ids_old:
            new_rows.append(r)

    lo_n, hi_n = late_sale_eligible_sale_date_range(d_new)
    lo_o, hi_o = late_sale_eligible_sale_date_range(d_old)

    return {
        "spreadsheet_title": ss_title,
        "spreadsheet_id": spreadsheet_id,
        "tab_new": tab_new,
        "tab_old": tab_old,
        "d_new": d_new,
        "d_old": d_old,
        "window_new": (lo_n, hi_n),
        "window_old": (lo_o, hi_o),
        "header": header,
        "drop_new": drop_new,
        "drop_old": drop_old,
        "dup_new": dup,
        "count_new_filtered": len(seen_new),
        "count_old_filtered": len(ids_old),
        "new_row_values": new_rows,
        "rows_new_filtered": rows_new_f,
        "ids_old_filtered": ids_old,
    }


def _merchant_backfill_hints() -> Tuple[str, ...]:
    from config import LATE_SALES_RAW_BACKFILL_MERCHANTS

    return tuple(
        x.strip().lower()
        for x in (LATE_SALES_RAW_BACKFILL_MERCHANTS or "").split(",")
        if x.strip()
    )


def _sale_dict_to_sheet_row(header: list[str], sale: Dict[str, Any]) -> list[str]:
    """Map ``workflows.kelkoo_sales_report`` sale dict to sheet row column order."""
    key_map = {
        "merchant": "merchant",
        "date": "date",
        "click_id": "click_id",
        "lead_valid": "lead_valid",
        "sale": "sale",
        "sale_value_usd": "sale_value_usd",
        "cpc": "cpc",
        "country": "country",
        "postback": "postback",
    }
    out: list[str] = []
    for col in header:
        k = key_map.get(col.strip().lower(), col)
        out.append(str(sale.get(k) or sale.get(col) or ""))
    return out


def _fetch_raw_sales_for_window(
    lo: date,
    hi: date,
    merchant_hints: Tuple[str, ...],
) -> List[Dict[str, Any]]:
    """Day-by-day Kelkoo raw export for merchant hints (catches sales missing from daily tabs)."""
    if not merchant_hints:
        return []
    from config import LATE_SALES_RAW_BACKFILL_GEOS, discover_kelkoo_feed_api_keys
    from integrations.daily_conversion_postbacks import fetch_kelkoo_raw_tsv
    from workflows.kelkoo_sales_report import _sale_rows_from_tsv

    session = requests.Session()
    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    geos = LATE_SALES_RAW_BACKFILL_GEOS
    day = lo
    while day <= hi:
        for feed_index, api_key in discover_kelkoo_feed_api_keys():
            for geo in geos:
                try:
                    status, body = fetch_kelkoo_raw_tsv(geo, day.isoformat(), api_key, session)
                except Exception as e:
                    logger.warning("Raw backfill %s %s %s: %s", feed_index, geo, day, e)
                    continue
                if status != 200:
                    continue
                for row in _sale_rows_from_tsv(body, feed_index):
                    m = str(row.get("merchant") or "").lower()
                    if not any(h in m for h in merchant_hints):
                        continue
                    cid = str(row.get("click_id") or "").strip()
                    if not cid or cid in seen:
                        continue
                    seen.add(cid)
                    out.append(row)
        day += timedelta(days=1)
    return out


def _collect_late_sale_candidate_rows(
    header: list[str],
    core: dict[str, Any],
    *,
    keitaro_latesale_ids: Set[str],
    keitaro_saleour_ids: Set[str],
    log_fired_ids: Set[str],
) -> Tuple[List[list[str]], dict[str, int]]:
    """
    Union of (1) day-over-day 7-day diff new rows and (2) sales on the latest 7-day tab
    with no SaleOur/LateSale in Keitaro, plus optional raw backfill for watchlist merchants.
    """
    from config import LATE_SALES_INCLUDE_MISSED_KEITARO

    idx_click = header.index("click_id")
    ids_old: Set[str] = set(core.get("ids_old_filtered") or [])
    rows_new_f: list[list[str]] = list(core.get("rows_new_filtered") or [])
    by_cid: Dict[str, list[str]] = {}

    for r in core.get("new_row_values") or []:
        cid = row_get(header, r, "click_id")
        if cid:
            by_cid[cid] = r

    missed_sheet = 0
    if LATE_SALES_INCLUDE_MISSED_KEITARO:
        for r in rows_new_f:
            cid = row_get(header, r, "click_id")
            if not cid or cid in by_cid:
                continue
            if cid in keitaro_latesale_ids or cid in keitaro_saleour_ids or cid in log_fired_ids:
                continue
            by_cid[cid] = r
            missed_sheet += 1

    lo_n, hi_n = core["window_new"]
    raw_added = 0
    hints = _merchant_backfill_hints()
    if hints:
        for sale in _fetch_raw_sales_for_window(lo_n, hi_n, hints):
            cid = str(sale.get("click_id") or "").strip()
            if not cid or cid in by_cid:
                continue
            if cid in keitaro_latesale_ids or cid in keitaro_saleour_ids or cid in log_fired_ids:
                continue
            by_cid[cid] = _sale_dict_to_sheet_row(header, sale)
            raw_added += 1

    stats = {
        "diff_new": len(core.get("new_row_values") or []),
        "missed_on_sheet": missed_sheet,
        "raw_backfill": raw_added,
        "total_candidates": len(by_cid),
    }
    return list(by_cid.values()), stats


def compute_new_late_sale_rows(
    *,
    spreadsheet_id: str,
    credentials_path: Path,
    as_of: date | None,
) -> dict[str, Any]:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    if not credentials_path.is_file():
        raise FileNotFoundError(f"credentials.json not found at {credentials_path}")

    creds = service_account.Credentials.from_service_account_file(str(credentials_path))
    service = build("sheets", "v4", credentials=creds).spreadsheets()
    meta = service.get(spreadsheetId=spreadsheet_id, fields="properties(title),sheets(properties(title))").execute()
    return _compute_new_late_sale_rows_inner(service, spreadsheet_id, meta, as_of)


def diff_rows_to_late_sale_rows(header: list[str], new_row_values: list[list[str]], postback_base: str) -> list[LateSaleDiffRow]:
    out: list[LateSaleDiffRow] = []
    for r in new_row_values:
        cid = row_get(header, r, "click_id")
        if not cid:
            continue
        sale_val = row_get(header, r, "sale_value_usd")
        url = build_postback_url(postback_base=postback_base, click_id=cid, sale_value_usd=sale_val)
        out.append(
            LateSaleDiffRow(
                click_id=cid,
                date=row_get(header, r, "date"),
                merchant=row_get(header, r, "merchant"),
                sale_value_usd=sale_val,
                country=row_get(header, r, "country"),
                postback_url=url,
            )
        )
    return out


def send_postback_gets(urls: list[str]) -> list[dict[str, Any]]:
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


def run_late_sales_flow(
    *,
    credentials_path: Path,
    spreadsheet_id: str,
    postback_base: str,
    as_of_str: str,
    apply: bool,
) -> dict[str, Any]:
    """
    Full flow for UI / tooling.

    ``as_of_str``: empty = latest tab; else ``YYYY-MM-DD`` = generation date of newer 7-day tab.
    """
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    as_of: date | None = None
    s = (as_of_str or "").strip()
    if s:
        as_of = datetime.strptime(s, "%Y-%m-%d").date()

    if not credentials_path.is_file():
        return {
            "ok": False,
            "error": f"credentials.json not found at {credentials_path}",
            "mode": "apply" if apply else "dry-run",
        }

    try:
        creds = service_account.Credentials.from_service_account_file(str(credentials_path))
        service = build("sheets", "v4", credentials=creds).spreadsheets()
        meta = service.get(spreadsheetId=spreadsheet_id, fields="properties(title),sheets(properties(title))").execute()
        core = _compute_new_late_sale_rows_inner(service, spreadsheet_id, meta, as_of)
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "mode": "apply" if apply else "dry-run",
        }

    header = core["header"]
    d_new: date = core["d_new"]
    d_old: date = core["d_old"]
    wn = core["window_new"]
    wo = core["window_old"]

    titles = sheet_titles(meta)
    skip_daily_tab = os.getenv("LATE_SALES_SKIP_IF_IN_DAILY_TAB", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    daily_ids: Set[str] = set()
    if skip_daily_tab:
        daily_ids = collect_daily_sale_click_ids(service, spreadsheet_id, meta)

    keitaro_latesale_ids: Set[str] = set()
    keitaro_saleour_ids: Set[str] = set()
    try:
        from config import KELKOO_LATE_SALES_KEITARO_LOOKBACK_DAYS
        from integrations.keitaro_conversions import (
            collect_keitaro_conversion_subids_by_status,
            collect_late_sale_dedup_subids,
        )

        lookback = int(KELKOO_LATE_SALES_KEITARO_LOOKBACK_DAYS)
        keitaro_latesale_ids = collect_late_sale_dedup_subids(lookback_days=lookback)
        by_st = collect_keitaro_conversion_subids_by_status(lookback_days=lookback)
        keitaro_saleour_ids = set(by_st.get("SaleOur") or set())
        logger.info(
            "Late-sales Keitaro dedup: LateSale=%s SaleOur=%s (lookback %sd)",
            len(keitaro_latesale_ids),
            len(keitaro_saleour_ids),
            lookback,
        )
    except Exception as e:
        logger.warning("Late-sales Keitaro dedup skipped: %s", e)

    pre_vals, _pre_stats = _collect_late_sale_candidate_rows(
        header,
        core,
        keitaro_latesale_ids=keitaro_latesale_ids,
        keitaro_saleour_ids=keitaro_saleour_ids,
        log_fired_ids=set(),
    )
    sale_dates: list[date | None] = []
    for r in pre_vals:
        sale_dates.append(parse_row_sale_date(row_get(header, r, "date")))
    log_fired_ids = collect_logged_fired_click_ids(service, spreadsheet_id, meta, d_new, sale_dates)

    candidate_vals, cand_stats = _collect_late_sale_candidate_rows(
        header,
        core,
        keitaro_latesale_ids=keitaro_latesale_ids,
        keitaro_saleour_ids=keitaro_saleour_ids,
        log_fired_ids=log_fired_ids,
    )

    rows = diff_rows_to_late_sale_rows(header, candidate_vals, postback_base)

    row_dicts: list[dict[str, Any]] = []
    for x in rows:
        cid = x.click_id
        skip = ""
        if cid in keitaro_latesale_ids:
            skip = "already_latesale_keitaro"
        elif cid in keitaro_saleour_ids:
            skip = "already_saleour_keitaro"
        elif cid in log_fired_ids:
            skip = "already_logged_late_postback"
        elif cid in daily_ids:
            skip = "already_in_daily_sheet"
        row_dicts.append(
            {
                "click_id": cid,
                "date": x.date,
                "merchant": x.merchant,
                "sale_value_usd": x.sale_value_usd,
                "country": x.country,
                "postback_url": x.postback_url,
                "skip_reason": skip,
                "http_status": None,
                "http_error": None,
            }
        )

    skip_keitaro = sum(
        1
        for r in row_dicts
        if r["skip_reason"] in ("already_latesale_keitaro", "already_saleour_keitaro")
    )
    skip_daily = sum(1 for r in row_dicts if r["skip_reason"] == "already_in_daily_sheet")
    skip_logged = sum(1 for r in row_dicts if r["skip_reason"] == "already_logged_late_postback")
    to_send = [r for r in row_dicts if not r["skip_reason"]]

    post_ok = 0
    post_fail = 0
    log_rows: list[list[Any]] = []
    fired_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log_title = month_late_log_sheet_title(d_new)

    if apply and to_send:
        urls = [r["postback_url"] for r in to_send]
        send_results = send_postback_gets(urls)
        for rd, sr in zip(to_send, send_results, strict=True):
            rd["http_status"] = sr.get("http_status")
            rd["http_error"] = sr.get("http_error")
            code = sr.get("http_status")
            ok_one = code is not None and 200 <= code < 300 and not sr.get("http_error")
            if ok_one:
                post_ok += 1
                sd = parse_row_sale_date(rd["date"])
                sale_date_str = str(sd) if sd else (rd["date"] or "")[:10]
                log_rows.append(
                    [
                        rd["click_id"],
                        sale_date_str,
                        rd["merchant"],
                        rd["sale_value_usd"],
                        rd["country"],
                        rd["postback_url"],
                        "yes",
                        fired_ts,
                        "late_diff",
                    ]
                )
            else:
                post_fail += 1

        if log_rows:
            try:
                ensure_late_log_sheet_with_header(service, spreadsheet_id, log_title, titles)
                append_late_log_rows(service, spreadsheet_id, log_title, log_rows)
            except Exception as e:
                return {
                    "ok": False,
                    "error": f"Postbacks sent but monthly log append failed: {e}",
                    "mode": "apply",
                    "spreadsheet_title": core["spreadsheet_title"],
                    "spreadsheet_id": core["spreadsheet_id"],
                    "tab_new": core["tab_new"],
                    "tab_old": core["tab_old"],
                    "d_new": str(d_new),
                    "d_old": str(d_old),
                    "window_new": f"{wn[0]} .. {wn[1]}",
                    "window_old": f"{wo[0]} .. {wo[1]}",
                    "drop_new": core["drop_new"],
                    "drop_old": core["drop_old"],
                    "dup_new": core["dup_new"],
                    "count_new_filtered": core["count_new_filtered"],
                    "count_old_filtered": core["count_old_filtered"],
                    "new_count": len(row_dicts),
                    "diff_count": cand_stats.get("diff_new", 0),
                    "missed_on_sheet": cand_stats.get("missed_on_sheet", 0),
                    "raw_backfill": cand_stats.get("raw_backfill", 0),
                    "candidate_count": cand_stats.get("total_candidates", 0),
                    "eligible_count": len(to_send),
                    "skipped_keitaro": skip_keitaro,
                    "skipped_daily": skip_daily,
                    "skipped_logged": skip_logged,
                    "postbacks_ok": post_ok,
                    "postbacks_fail": post_fail,
                    "log_sheet": log_title,
                    "log_rows_appended": 0,
                    "rows": row_dicts,
                }

    ok = (not apply) or (post_fail == 0)

    return {
        "ok": ok,
        "error": None,
        "mode": "apply" if apply else "dry-run",
        "spreadsheet_title": core["spreadsheet_title"],
        "spreadsheet_id": core["spreadsheet_id"],
        "tab_new": core["tab_new"],
        "tab_old": core["tab_old"],
        "d_new": str(d_new),
        "d_old": str(d_old),
        "window_new": f"{wn[0]} .. {wn[1]}",
        "window_old": f"{wo[0]} .. {wo[1]}",
        "drop_new": core["drop_new"],
        "drop_old": core["drop_old"],
        "dup_new": core["dup_new"],
        "count_new_filtered": core["count_new_filtered"],
        "count_old_filtered": core["count_old_filtered"],
        "new_count": len(row_dicts),
        "diff_count": cand_stats.get("diff_new", 0),
        "missed_on_sheet": cand_stats.get("missed_on_sheet", 0),
        "raw_backfill": cand_stats.get("raw_backfill", 0),
        "candidate_count": cand_stats.get("total_candidates", 0),
        "eligible_count": len(to_send),
        "skipped_keitaro": skip_keitaro,
        "skipped_daily": skip_daily,
        "skipped_logged": skip_logged,
        "postbacks_ok": post_ok if apply else None,
        "postbacks_fail": post_fail if apply else None,
        "log_sheet": log_title,
        "log_rows_appended": len(log_rows) if apply else None,
        "rows": row_dicts,
    }
