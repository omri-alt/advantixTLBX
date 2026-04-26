"""
Kelkoo late-sales (KLtools): diff last two ``SalesReport_7days-generated-*`` tabs,
apply sale-date window rules, build postback URLs, optionally GET each URL.

Dedup before sending (apply or dry-run display):
  - ``SalesReport_<saleday>_generated-<genday>`` or ``SalesReport_feed{n}_<saleday>_generated-<genday>``
    daily exports: ``click_id`` already present (on-time sale; original postback already fired).
  - ``{month}_late_sales_log`` tabs: ``click_id`` already has ``late_postback_fired_at_utc`` set
    (we already sent a LateSale postback for that sale).

After successful LateSale GETs (apply), append rows to ``{month}_late_sales_log`` for the
**generation month** of the newer 7-day tab (``d_new``), e.g. ``april_late_sales_log``.

Used by ``tools/kelkoo_late_sales_7day_diff.py`` and the Flask UI.
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit

import requests

TAB_RE = re.compile(r"^SalesReport_7days-generated-(\d{4}-\d{2}-\d{2})$")
DAILY_TAB_RE = re.compile(
    r"^SalesReport_(?:(feed\d+)_)?(\d{4}-\d{2}-\d{2})_generated-(\d{4}-\d{2}-\d{2})$",
    re.I,
)

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
    }


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
    new_vals = core["new_row_values"]
    d_new: date = core["d_new"]
    d_old: date = core["d_old"]
    wn = core["window_new"]
    wo = core["window_old"]

    titles = sheet_titles(meta)
    daily_ids = collect_daily_sale_click_ids(service, spreadsheet_id, meta)

    sale_dates: list[date | None] = []
    for r in new_vals:
        sale_dates.append(parse_row_sale_date(row_get(header, r, "date")))
    log_fired_ids = collect_logged_fired_click_ids(service, spreadsheet_id, meta, d_new, sale_dates)

    rows = diff_rows_to_late_sale_rows(header, new_vals, postback_base)

    row_dicts: list[dict[str, Any]] = []
    for x in rows:
        cid = x.click_id
        skip = ""
        if cid in daily_ids:
            skip = "already_in_daily_sheet"
        elif cid in log_fired_ids:
            skip = "already_logged_late_postback"
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
                    "diff_count": len(row_dicts),
                    "eligible_count": len(to_send),
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
        "diff_count": len(row_dicts),
        "eligible_count": len(to_send),
        "skipped_daily": skip_daily,
        "skipped_logged": skip_logged,
        "postbacks_ok": post_ok if apply else None,
        "postbacks_fail": post_fail if apply else None,
        "log_sheet": log_title,
        "log_rows_appended": len(log_rows) if apply else None,
        "rows": row_dicts,
    }
