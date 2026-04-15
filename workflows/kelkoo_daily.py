"""
Kelkoo daily workflow: download merchants feed, segment by reports (red/yellow/green),
pick one green merchant per geo, generate offers sheet. Used by run_daily_workflow.py.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Union

import requests

logger = logging.getLogger(__name__)

# Geos used for Kelkoo feed (match Apps Script); 2nd geo after Norway is Spain (es)
KELKOO_FEED_GEOS = [
    "at", "be", "ch",
    "fi", "fr", "gr", "de", "hu", "id", "ie", "in", "it", "mx",
    "nl", "no", "es", "nz", "pl", "pt", "ro", "se", "sk", "uk", "us",
    "cz",
]

MERCHANTS_URL = "https://api.kelkoogroup.net/publisher/shopping/v2/feeds/merchants"
REPORTS_AGGREGATED_URL = "https://api.kelkoogroup.net/publisher/reports/v1/aggregated"
PLA_FEED_URL = "https://api.kelkoogroup.net/publisher/shopping/v2/feeds/pla"

# Colors (hex -> RGB 0-1)
COLOR_RED = {"red": 0.92, "green": 0.6, "blue": 0.6}       # #ea9999 closed / tested
COLOR_LIGHT_RED = {"red": 0.96, "green": 0.8, "blue": 0.8}   # #f4cccc >= 800 leads
COLOR_ORANGE = {"red": 0.99, "green": 0.9, "blue": 0.8}      # #fce5cd >= 400 leads
COLOR_YELLOW = {"red": 1, "green": 0.95, "blue": 0.8}        # #fff2cc >= 1 lead
COLOR_BLUE = {"red": 0.81, "green": 0.89, "blue": 0.95}      # #cfe2f3 sales > 0
COLOR_GREEN = {"red": 0.85, "green": 0.92, "blue": 0.83}     # #d9ead3 ready to go

CPC_FLOOR = 0.02

# Kelkoo reports use leadCount (often shown as "leads" on sheets); coloring uses the same.
LEADS_LOW_MAX = 399  # < 400 leads → green or yellow row when colored
# Orange rows (400–799 leads in ``apply_fixim_colors``) are **never** chosen — no more traffic.

CLICK_COUNT_HEADERS = {
    # Kelkoo / sheet naming (clicks column often = leads in practice)
    "leads",
    "leadcount",
    "lead_count",
    "clicks",
    "clickcount",
    "click_count",
    "clicks_this_month",
    "clicksthismonth",
    "this_month_clicks",
    "thismonthclicks",
    "month_clicks",
    "monthclicks",
    "monthlyclicks",
    "sentclicks",
    "sentclickcount",
    "sent_clicks",
}


def _headers(api_key: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}


def download_merchants_feed(
    api_key: str,
    geos: Optional[List[str]] = None,
    *,
    static_only: bool = True,
) -> List[Dict[str, Any]]:
    """Fetch merchants for each geo. Returns list of merchant dicts with geo_origin added."""
    geos = geos or KELKOO_FEED_GEOS
    all_merchants: List[Dict[str, Any]] = []
    all_keys: set = {"geo_origin"}
    skipped_forbidden: List[str] = []

    for geo in geos:
        try:
            r = requests.get(
                MERCHANTS_URL,
                params={"country": geo, "format": "JSON"},
                headers=_headers(api_key),
                timeout=30,
            )
            if r.status_code == 403:
                # Common when a publisher key has only a subset of markets (e.g. feed2 vs feed1).
                skipped_forbidden.append(geo)
                continue
            if r.status_code != 200:
                logger.warning("Merchants %s: status %s", geo, r.status_code)
                continue
            data = r.json()
            if not isinstance(data, list):
                continue
            for m in data:
                if static_only and m.get("merchantTier") != "Static":
                    continue
                m = dict(m)
                m["geo_origin"] = geo
                for k in m:
                    all_keys.add(k)
                all_merchants.append(m)
        except Exception as e:
            logger.warning("Merchants %s: %s", geo, e)
    if skipped_forbidden:
        skipped_forbidden.sort()
        tail = ", ".join(skipped_forbidden[:24])
        if len(skipped_forbidden) > 24:
            tail += f", … (+{len(skipped_forbidden) - 24} more)"
        logger.info(
            "Merchants feed: skipped %d country request(s) with HTTP 403 (not available for this API key): %s",
            len(skipped_forbidden),
            tail,
        )
    return all_merchants


def _all_keys_ordered(merchants: List[Dict], extra_first: List[str]) -> List[str]:
    order = list(extra_first)
    seen = set(order)
    for m in merchants:
        for k in m:
            if k not in seen:
                order.append(k)
                seen.add(k)
    return order


def _cell_value(v: Any) -> Union[str, int, float, bool]:
    """Ensure value is a scalar for Sheets API (no lists/dicts)."""
    if v is None:
        return ""
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, list):
        return ", ".join(str(_cell_value(x)) for x in v) if v else ""
    if isinstance(v, dict):
        return str(v)
    return str(v)


def _is_retryable_sheets_error(e: Exception) -> bool:
    """True if we should retry the Sheets API call (connection reset, server errors)."""
    if isinstance(e, ConnectionResetError):
        return True
    if isinstance(e, OSError) and getattr(e, "winerror", None) == 10054:
        return True
    if isinstance(e, OSError) and getattr(e, "errno", None) in (54, 104):  # ECONNRESET
        return True
    if hasattr(e, "resp") and getattr(e.resp, "status", None) in (500, 502, 503):
        return True
    return False


def _sheets_execute(request, max_retries: int = 3):
    """Execute a Sheets API request with retries on connection errors."""
    last_err = None
    for attempt in range(max_retries):
        try:
            return request.execute()
        except Exception as e:
            last_err = e
            if attempt < max_retries - 1 and _is_retryable_sheets_error(e):
                delay = 2 * (attempt + 1)
                logger.warning("Sheets API transient error (attempt %s/%s), retrying in %ss: %s",
                    attempt + 1, max_retries, delay, e)
                time.sleep(delay)
                continue
            raise last_err
    raise last_err


def write_fixim_sheet(
    service: Any,
    spreadsheet_id: str,
    sheet_name: str,
    merchants: List[Dict[str, Any]],
) -> None:
    """Write merchants to sheet. Create sheet if missing; columns = all keys (geo_origin first)."""
    if not merchants:
        return
    headers = _all_keys_ordered(merchants, ["geo_origin"])
    rows = [headers]
    for m in merchants:
        rows.append([_cell_value(m.get(h)) for h in headers])

    meta = _sheets_execute(
        service.get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
    )
    titles = [s["properties"]["title"] for s in meta.get("sheets", [])]
    if sheet_name not in titles:
        _sheets_execute(
            service.batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]},
            )
        )

    quoted = sheet_name.replace("'", "''")
    _sheets_execute(
        service.values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"'{quoted}'!A1:ZZ10000",
        )
    )
    _sheets_execute(
        service.values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{quoted}'!A1",
            valueInputOption="RAW",
            body={"values": rows},
        )
    )
    logger.info("Wrote %s rows to %s", len(rows), sheet_name)


def fetch_reports(
    api_key: str,
    start_date: str,
    end_date: str,
) -> Dict[str, Dict[str, int]]:
    """Fetch aggregated report by merchantId. Returns {merchant_id: {leads, sales}}."""
    r = requests.get(
        REPORTS_AGGREGATED_URL,
        params={"start": start_date, "end": end_date, "groupBy": "merchantId", "format": "JSON"},
        headers=_headers(api_key),
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Reports API {r.status_code}: {r.text[:500]}")
    data = r.json()
    out = {}
    for item in data:
        mid = item.get("merchantId")
        if mid is not None:
            out[str(mid)] = {
                "leads": int(item.get("leadCount") or 0),
                "sales": int(item.get("saleCount") or 0),
            }
    return out


def fetch_report_merchant_names(
    api_key: str,
    start_date: str,
    end_date: str,
) -> Dict[str, str]:
    """
    Kelkoo aggregated report by merchantId: map merchantId -> merchantName (non-empty only).

    Same API as ``fetch_reports``; use the same date range as month-to-date coloring when
    resolving display names for a calendar month.
    """
    r = requests.get(
        REPORTS_AGGREGATED_URL,
        params={"start": start_date, "end": end_date, "groupBy": "merchantId", "format": "JSON"},
        headers=_headers(api_key),
        timeout=60,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Reports API {r.status_code}: {r.text[:500]}")
    out: Dict[str, str] = {}
    for item in r.json() or []:
        mid = item.get("merchantId")
        if mid is None:
            continue
        name = str(item.get("merchantName") or "").strip()
        if not name:
            continue
        out[str(mid)] = name
    return out


def build_merchant_id_to_name_from_feed(
    api_key: str,
    geos: Optional[List[str]] = None,
) -> Dict[str, str]:
    """
    Map merchant id (and ``websiteId``) -> display ``name`` from the live merchants feed.

    Uses ``static_only=False`` so ids that only appear in reports can still resolve when
    Kelkoo returns them in the feed (same idea as ``blend_potential_merchants``).

    ``geos``: optional subset of ``KELKOO_FEED_GEOS`` (e.g. feed2-only markets from config).
    """
    merchants = download_merchants_feed(api_key, geos, static_only=False)
    out: Dict[str, str] = {}
    for m in merchants:
        name = str(m.get("name") or "").strip()
        if not name:
            continue
        for key in (m.get("id"), m.get("websiteId")):
            if key is None:
                continue
            ks = str(key)
            if ks not in out:
                out[ks] = name
    return out


def _hex_to_rgb(hex_str: str) -> Dict[str, float]:
    h = hex_str.lstrip("#")
    if len(h) != 6:
        return {"red": 1, "green": 1, "blue": 1}
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return {"red": r / 255.0, "green": g / 255.0, "blue": b / 255.0}


def apply_fixim_colors(
    service: Any,
    spreadsheet_id: str,
    sheet_name: str,
    performance_map: Dict[str, Dict[str, int]],
) -> None:
    """Read fixim sheet, compute row colors from visible + performance, set backgrounds."""
    quoted = sheet_name.replace("'", "''")
    try:
        result = service.values().get(
            spreadsheetId=spreadsheet_id,
            range=f"'{quoted}'!A:Z",
        ).execute()
    except Exception:
        logger.warning("Could not read sheet %s for coloring", sheet_name)
        return
    values = result.get("values") or []
    if not values:
        return
    headers = values[0]
    id_idx = next((i for i, h in enumerate(headers) if (h or "").strip().lower() == "id"), -1)
    visible_idx = next((i for i, h in enumerate(headers) if (h or "").strip().lower() == "visible"), -1)
    if id_idx == -1:
        logger.warning("No 'id' column in %s", sheet_name)
        return

    meta = service.get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))").execute()
    sheet_id = None
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == sheet_name:
            sheet_id = s["properties"]["sheetId"]
            break
    if sheet_id is None:
        return

    requests_batch = []
    for i in range(1, len(values)):
        row = values[i]
        mid = _normalize_merchant_id_from_sheet(row[id_idx] if id_idx < len(row) else None)
        is_visible = True
        if visible_idx >= 0 and visible_idx < len(row):
            v = row[visible_idx]
            if v is False or (isinstance(v, str) and v.strip().upper() == "FALSE"):
                is_visible = False
        perf = performance_map.get(mid, {"leads": 0, "sales": 0})
        leads, sales = perf["leads"], perf["sales"]

        if not is_visible:
            color = COLOR_RED
        elif sales > 0:
            color = COLOR_BLUE
        elif leads >= 800:
            color = COLOR_LIGHT_RED
        elif leads >= 400:
            color = COLOR_ORANGE
        elif leads >= 1:
            color = COLOR_YELLOW
        else:
            color = COLOR_GREEN

        requests_batch.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": i,
                    "endRowIndex": i + 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": len(headers),
                },
                "cell": {
                    "userEnteredFormat": {"backgroundColor": color},
                },
                "fields": "userEnteredFormat.backgroundColor",
            },
        })

    if requests_batch:
        service.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests_batch},
        ).execute()
    logger.info("Applied colors to %s (%d rows)", sheet_name, len(requests_batch))


def _is_green_bg(bg: Optional[Dict]) -> bool:
    """True if backgroundColor matches #d9ead3 (light green)."""
    if not bg:
        return False
    r, g, b = bg.get("red", 0), bg.get("green", 0), bg.get("blue", 0)
    return 0.82 <= r <= 0.88 and 0.9 <= g <= 0.95 and 0.80 <= b <= 0.86


def _is_light_yellow_bg(bg: Optional[Dict]) -> bool:
    """True if backgroundColor matches #fff2cc (light yellow)."""
    if not bg:
        return False
    r, g, b = bg.get("red", 0), bg.get("green", 0), bg.get("blue", 0)
    # COLOR_YELLOW = {"red": 1, "green": 0.95, "blue": 0.8}
    return 0.95 <= r <= 1.0 and 0.88 <= g <= 0.98 and 0.74 <= b <= 0.86


def _is_orange_bg(bg: Optional[Dict]) -> bool:
    """True if backgroundColor matches #fce5cd (orange = 400–799 Kelkoo leads; not selectable)."""
    if not bg:
        return False
    r, g, b = bg.get("red", 0), bg.get("green", 0), bg.get("blue", 0)
    # COLOR_ORANGE = {"red": 0.99, "green": 0.9, "blue": 0.8}
    return 0.95 <= r <= 1.0 and 0.84 <= g <= 0.95 and 0.74 <= b <= 0.86


def _parse_click_count(v: Any) -> int:
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        try:
            return int(float(v))
        except Exception:
            return 0
    s = str(v).strip()
    if not s:
        return 0
    # remove common formatting
    s = s.replace(",", "")
    try:
        return int(float(s))
    except Exception:
        return 0


def _parse_cpc_value(v: Any) -> float:
    """Parse CPC from sheet/API cell; supports '0,05' (EU decimal) and plain numbers."""
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        try:
            return float(v)
        except Exception:
            return 0.0
    s = str(v).strip().replace(" ", "")
    if not s or s in ("-", "—", "n/a", "na"):
        return 0.0
    # European decimal comma (single comma as decimal separator)
    if s.count(",") == 1 and s.count(".") == 0:
        s = s.replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _row_optional_click_count(row: List[Any], click_idx: int) -> Optional[int]:
    """
    If the sheet has a leads/clicks column: return int when the cell has a value,
    or None when empty (unknown — tiering falls back to row color only).
    """
    if click_idx < 0 or click_idx >= len(row):
        return None
    raw = row[click_idx]
    if raw is None:
        return None
    if isinstance(raw, str) and not raw.strip():
        return None
    return _parse_click_count(raw)


def _normalize_merchant_id_from_sheet(raw: Any) -> str:
    """
    Google Sheets often returns numeric ids as floats (e.g. 15248713.0). Kelkoo report keys
    and PLA merchantId expect the plain integer string (15248713) or API rejects / lead
    counts fail to match.
    """
    if raw is None:
        return ""
    if isinstance(raw, bool):
        return ""
    if isinstance(raw, int):
        return str(raw)
    if isinstance(raw, float):
        if raw.is_integer():
            return str(int(raw))
        return str(raw).strip()
    s = str(raw).strip()
    if not s:
        return ""
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except (ValueError, OverflowError):
        pass
    return s


def build_pla_id_alternates_for_feed(merchants: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Kelkoo PLA ``merchantId`` sometimes must be ``websiteId`` from the merchants feed, not ``id``.
    Map normalized feed ``id`` -> normalized ``websiteId`` when they differ (try ``id`` first in PLA).
    """
    out: Dict[str, str] = {}
    for m in merchants:
        pid = _normalize_merchant_id_from_sheet(m.get("id"))
        wid = _normalize_merchant_id_from_sheet(m.get("websiteId"))
        if pid and wid and pid != wid:
            out[pid] = wid
    return out


def _cpc_passes_floor(
    desktop_cpc: float,
    mobile_cpc: float,
    *,
    has_desktop_value: bool,
    has_mobile_value: bool,
    require_both_channels: bool,
) -> bool:
    """
    When both CPC values are present on a row, **both** must be >= CPC_FLOOR.

    Avoids choosing merchants where e.g. merchantEstimatedCpc > 2¢ but
    merchantMobileEstimatedCpc is only ~1.6¢ (common US skew).

    If one CPC value is missing for a row, only the available value is checked.
    """
    if has_desktop_value and has_mobile_value:
        if require_both_channels:
            return desktop_cpc >= CPC_FLOOR and mobile_cpc >= CPC_FLOOR
        # For most geos, accept a merchant when at least one channel meets the floor.
        return max(desktop_cpc, mobile_cpc) >= CPC_FLOOR
    if has_desktop_value:
        return desktop_cpc >= CPC_FLOOR
    if has_mobile_value:
        return mobile_cpc >= CPC_FLOOR
    return False


def _has_cpc_cell_value(v: Any) -> bool:
    """True when a CPC cell is meaningfully present (not blank / dash / n/a)."""
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return True
    s = str(v).strip().lower()
    return s not in ("", "-", "—", "n/a", "na")


def _row_is_visible_merchant(row: List[Any], visible_idx: int) -> bool:
    """Same rule as apply_fixim_colors: FALSE / false in visible column → not selectable."""
    if visible_idx < 0 or visible_idx >= len(row):
        return True
    v = row[visible_idx]
    if v is False:
        return False
    if isinstance(v, str) and v.strip().upper() == "FALSE":
        return False
    return True


def pick_merchants_one_per_geo_from_fixim_values(
    values: List[List[Any]],
    performance_map: Dict[str, Dict[str, int]],
    *,
    log_context: str = "",
) -> Dict[str, str]:
    """
    Core selection: same rules as ``get_green_merchants_one_per_geo`` (report + sheet rows).
    """
    if len(values) < 2:
        return {}
    headers = values[0]
    geo_idx = next((i for i, h in enumerate(headers) if (h or "").strip().lower() == "geo_origin"), -1)
    id_idx = next((i for i, h in enumerate(headers) if (h or "").strip().lower() == "id"), -1)
    visible_idx = next((i for i, h in enumerate(headers) if (h or "").strip().lower() == "visible"), -1)
    cpc_idx = next((i for i, h in enumerate(headers) if (h or "").strip().lower() == "cpc_desktop"), -1)
    if cpc_idx == -1:
        cpc_idx = next((i for i, h in enumerate(headers) if (h or "").strip().lower() == "merchantestimatedcpc"), -1)
    mobile_cpc_idx = next((i for i, h in enumerate(headers) if (h or "").strip().lower() == "cpc_mobile"), -1)
    if mobile_cpc_idx == -1:
        mobile_cpc_idx = next((i for i, h in enumerate(headers) if (h or "").strip().lower() == "merchantmobileestimatedcpc"), -1)
    if geo_idx == -1 or id_idx == -1:
        return {}

    if not performance_map and log_context:
        logger.warning(
            "Empty performance_map%s; merchants without report rows treated as 0 leads / 0 sales",
            f" ({log_context})" if log_context else "",
        )

    by_geo_eligible: Dict[str, List[tuple]] = {}
    for i in range(1, len(values)):
        row = values[i]
        if geo_idx >= len(row) or id_idx >= len(row):
            continue
        geo = (row[geo_idx] or "").strip().lower()
        if not geo:
            continue
        merchant_id = _normalize_merchant_id_from_sheet(row[id_idx] if id_idx < len(row) else None)
        if not merchant_id:
            continue
        if not _row_is_visible_merchant(row, visible_idx):
            continue

        perf = performance_map.get(merchant_id, {"leads": 0, "sales": 0})
        leads = int(perf.get("leads", 0) or 0)
        sales = int(perf.get("sales", 0) or 0)
        if leads > LEADS_LOW_MAX:
            continue

        desktop_raw = row[cpc_idx] if cpc_idx >= 0 and cpc_idx < len(row) else None
        mobile_raw = row[mobile_cpc_idx] if mobile_cpc_idx >= 0 and mobile_cpc_idx < len(row) else None
        desktop_cpc = _parse_cpc_value(desktop_raw)
        mobile_cpc = _parse_cpc_value(mobile_raw)
        require_both_channels = (geo == "us")
        if not _cpc_passes_floor(
            desktop_cpc,
            mobile_cpc,
            has_desktop_value=_has_cpc_cell_value(desktop_raw),
            has_mobile_value=_has_cpc_cell_value(mobile_raw),
            require_both_channels=require_both_channels,
        ):
            continue

        by_geo_eligible.setdefault(geo, []).append((merchant_id, desktop_cpc, mobile_cpc))

    mapping: Dict[str, str] = {}
    for geo, candidates in by_geo_eligible.items():
        best = max(candidates, key=lambda x: (x[1], x[2]))
        mapping[geo] = best[0]
    return mapping


def pick_top_merchants_per_geo_from_fixim_values(
    values: List[List[Any]],
    performance_map: Dict[str, Dict[str, int]],
    *,
    top_n: int = 3,
    log_context: str = "",
) -> Dict[str, List[str]]:
    """
    Same eligibility rules as ``pick_merchants_one_per_geo_from_fixim_values`` but returns
    the top-N merchant ids per geo (sorted by CPC).
    """
    if top_n <= 0:
        return {}
    if len(values) < 2:
        return {}

    headers = values[0]
    geo_idx = next((i for i, h in enumerate(headers) if (h or "").strip().lower() == "geo_origin"), -1)
    id_idx = next((i for i, h in enumerate(headers) if (h or "").strip().lower() == "id"), -1)
    visible_idx = next((i for i, h in enumerate(headers) if (h or "").strip().lower() == "visible"), -1)
    cpc_idx = next((i for i, h in enumerate(headers) if (h or "").strip().lower() == "cpc_desktop"), -1)
    if cpc_idx == -1:
        cpc_idx = next((i for i, h in enumerate(headers) if (h or "").strip().lower() == "merchantestimatedcpc"), -1)
    mobile_cpc_idx = next((i for i, h in enumerate(headers) if (h or "").strip().lower() == "cpc_mobile"), -1)
    if mobile_cpc_idx == -1:
        mobile_cpc_idx = next((i for i, h in enumerate(headers) if (h or "").strip().lower() == "merchantmobileestimatedcpc"), -1)
    if geo_idx == -1 or id_idx == -1:
        return {}

    has_report = bool(performance_map)
    if not has_report and log_context:
        logger.warning(
            "Empty performance_map%s; merchants without report rows treated as 0 leads / 0 sales",
            f" ({log_context})" if log_context else "",
        )

    by_geo_eligible: Dict[str, List[tuple]] = {}
    for i in range(1, len(values)):
        row = values[i]
        if geo_idx >= len(row) or id_idx >= len(row):
            continue
        geo = (row[geo_idx] or "").strip().lower()
        if not geo:
            continue
        merchant_id = _normalize_merchant_id_from_sheet(row[id_idx] if id_idx < len(row) else None)
        if not merchant_id:
            continue
        if not _row_is_visible_merchant(row, visible_idx):
            continue

        perf = performance_map.get(merchant_id, {"leads": 0, "sales": 0})
        leads = int(perf.get("leads", 0) or 0)
        if leads > LEADS_LOW_MAX:
            continue

        desktop_raw = row[cpc_idx] if cpc_idx >= 0 and cpc_idx < len(row) else None
        mobile_raw = row[mobile_cpc_idx] if mobile_cpc_idx >= 0 and mobile_cpc_idx < len(row) else None
        desktop_cpc = _parse_cpc_value(desktop_raw)
        mobile_cpc = _parse_cpc_value(mobile_raw)

        require_both_channels = (geo == "us")
        if not _cpc_passes_floor(
            desktop_cpc,
            mobile_cpc,
            has_desktop_value=_has_cpc_cell_value(desktop_raw),
            has_mobile_value=_has_cpc_cell_value(mobile_raw),
            require_both_channels=require_both_channels,
        ):
            continue

        by_geo_eligible.setdefault(geo, []).append((merchant_id, desktop_cpc, mobile_cpc))

    mapping: Dict[str, List[str]] = {}
    for geo, candidates in by_geo_eligible.items():
        # Sort by desktop_cpc desc then mobile_cpc desc
        candidates_sorted = sorted(candidates, key=lambda x: (x[1], x[2]), reverse=True)
        seen_ids = set()
        top_ids: List[str] = []
        for mid, _, _ in candidates_sorted:
            if mid in seen_ids:
                continue
            seen_ids.add(mid)
            top_ids.append(mid)
            if len(top_ids) >= top_n:
                break
        if top_ids:
            mapping[geo] = top_ids
    return mapping


def get_green_merchants_one_per_geo(
    service: Any,
    spreadsheet_id: str,
    sheet_name: str,
    performance_map: Dict[str, Dict[str, int]],
) -> Dict[str, str]:
    """
    Pick one merchant per geo for PLA offers.

    **Source of truth** is the same Kelkoo month-to-date aggregated report used to color
    the sheet (``fetch_reports`` → ``leadCount`` / ``saleCount``). We do **not** rely on
    reading row background RGB from Sheets (can disagree with the API).

    Eligible merchant for a country:
      - Row is **visible** (same as ``apply_fixim_colors``).
      - **leads** <= LEADS_LOW_MAX (399). So 400+ leads (orange / light-red in coloring) → no traffic.
      - CPC floor:
        - for most geos: if both CPC cells are present, at least one channel must be >= CPC_FLOOR
        - for `us` only: if both cells are present, both must be >= CPC_FLOOR
        - if one CPC cell is missing/blank, only the present one must meet the floor.

    Merchant ids from the sheet are normalized (e.g. ``15248713.0`` → ``15248713``) so report
    lead counts and PLA requests match Kelkoo.

    Per geo: choose the eligible merchant with highest CPC (desktop, then mobile tie-break).
    Geos with no eligible merchant are omitted (no offers for that country).
    """
    quoted = sheet_name.replace("'", "''")
    try:
        values = service.values().get(
            spreadsheetId=spreadsheet_id,
            range=f"'{quoted}'!A:ZZ",
        ).execute().get("values") or []
    except Exception:
        return {}
    return pick_merchants_one_per_geo_from_fixim_values(
        values, performance_map, log_context=sheet_name
    )


def get_top_merchants_per_geo(
    service: Any,
    spreadsheet_id: str,
    sheet_name: str,
    performance_map: Dict[str, Dict[str, int]],
    *,
    top_n: int = 3,
) -> Dict[str, List[str]]:
    """Select top-N merchants per geo using the same report-based eligibility."""
    quoted = sheet_name.replace("'", "''")
    try:
        values = service.values().get(
            spreadsheetId=spreadsheet_id,
            range=f"'{quoted}'!A:ZZ",
        ).execute().get("values") or []
    except Exception:
        return {}
    return pick_top_merchants_per_geo_from_fixim_values(
        values, performance_map, top_n=top_n, log_context=sheet_name
    )


def get_green_merchants_one_per_geo_from_values(
    values: List[List[Any]],
    performance_map: Optional[Dict[str, Dict[str, int]]] = None,
) -> Dict[str, str]:
    """
    Same selection as ``get_green_merchants_one_per_geo`` using in-memory sheet rows +
    Kelkoo ``performance_map`` (merchant id -> {leads, sales}).
    """
    return pick_merchants_one_per_geo_from_fixim_values(
        values, performance_map or {}, log_context="from_values"
    )


def _pla_tsv_lines_for_merchant(
    api_key: str,
    geo: str,
    merchant_id: str,
    *,
    number_of_parts: int = 8,
    alternate_merchant_ids: Optional[List[str]] = None,
) -> List[str]:
    """
    Kelkoo splits large PLA feeds across ``numberOfParts``; part 1 is often empty while
    later parts hold products. Return lines from the first (id, part) that yields data.

    TSV is accepted when there is a header plus **at least one** product row (``len(lines) > 1``).

    Tries ``merchant_id`` first, then each ``alternate_merchant_ids`` entry (e.g. ``websiteId``).
    """
    geo_l = (geo or "").strip().lower()
    primary = _normalize_merchant_id_from_sheet(merchant_id)
    if not primary:
        return []
    ids_to_try: List[str] = [primary]
    for alt in alternate_merchant_ids or []:
        a = _normalize_merchant_id_from_sheet(alt)
        if a and a not in ids_to_try:
            ids_to_try.append(a)

    last_status: Optional[int] = None
    last_snippet = ""
    for try_mid in ids_to_try:
        for part in range(1, number_of_parts + 1):
            try:
                r = requests.get(
                    PLA_FEED_URL,
                    params={
                        "country": geo_l,
                        "merchantId": try_mid,
                        "format": "JSON",
                        "numberOfParts": number_of_parts,
                        "part": part,
                    },
                    headers={**_headers(api_key), "Accept": "text/tab-separated-values"},
                    timeout=45,
                )
            except Exception as e:
                logger.warning("PLA %s merchant %s part %s: %s", geo_l, try_mid, part, e)
                continue
            last_status = r.status_code
            last_snippet = (r.text or "")[:240]
            if r.status_code != 200:
                logger.warning(
                    "PLA %s merchant %s part %s: HTTP %s",
                    geo_l,
                    try_mid,
                    part,
                    r.status_code,
                )
                continue
            text = (r.text or "").strip()
            if not text:
                continue
            lines = text.split("\n")
            if len(lines) > 1:
                if try_mid != primary:
                    logger.info(
                        "PLA %s: products found with alternate merchantId=%s (primary id=%s)",
                        geo_l,
                        try_mid,
                        primary,
                    )
                return lines

    logger.warning(
        "PLA %s merchant id(s) %s: no product rows (tried parts 1-%s; last HTTP=%s, body_prefix=%r)",
        geo_l,
        ids_to_try,
        number_of_parts,
        last_status,
        last_snippet[:120],
    )
    return []


def generate_offers(
    api_key: str,
    geo_to_merchant_id: Dict[str, str],
    max_products_per_geo: int = 100,
    *,
    pla_id_alternates: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """For each (geo, merchant_id) fetch PLA feed and build offer rows (Country, Merchant ID, Product Title, Store Link, Audit Status, Timestamp)."""
    master: List[Dict[str, Any]] = []
    for geo, m_id in geo_to_merchant_id.items():
        try:
            nid = _normalize_merchant_id_from_sheet(m_id)
            alt = pla_id_alternates.get(nid) if pla_id_alternates and nid else None
            lines = _pla_tsv_lines_for_merchant(
                api_key,
                geo,
                str(m_id),
                alternate_merchant_ids=[alt] if alt else None,
            )
            if len(lines) <= 1:
                continue
            for line in lines[1 : max_products_per_geo + 1]:
                line = line.strip()
                if not line:
                    continue
                parts = line.split("\t")
                direct_link = (parts[2].replace('"', '').strip() if len(parts) > 2 else "") or ""
                if not direct_link.startswith("http"):
                    for p in parts:
                        p = p.strip()
                        if p.startswith("http"):
                            direct_link = p
                            break
                if not direct_link:
                    direct_link = "Link Not Found"
                title = (parts[1].replace('"', '') if len(parts) > 1 else "N/A").strip()
                master.append({
                    "Country": geo.upper(),
                    "Merchant ID": m_id,
                    "Product Title": title,
                    "Store Link": direct_link,
                    "Audit Status": "Active",
                    "Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                })
        except Exception as e:
            logger.warning("PLA %s: %s", geo, e)
    return master


def generate_offers_with_fallback(
    api_key: str,
    geo_to_merchant_ids: Dict[str, List[str]],
    max_products_per_geo: int = 100,
    *,
    pla_id_alternates: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Generate offers per geo by trying multiple merchants in order until we fill
    ``max_products_per_geo`` items (or merchant candidates are exhausted).
    """
    master: List[Dict[str, Any]] = []
    for geo, merchant_ids in geo_to_merchant_ids.items():
        produced = 0
        for m_id in merchant_ids:
            if produced >= max_products_per_geo:
                break
            if not m_id:
                continue
            try:
                nid = _normalize_merchant_id_from_sheet(m_id)
                alt = pla_id_alternates.get(nid) if pla_id_alternates and nid else None
                lines = _pla_tsv_lines_for_merchant(
                    api_key,
                    geo,
                    str(m_id),
                    alternate_merchant_ids=[alt] if alt else None,
                )
                if len(lines) <= 1:
                    continue
                for line in lines[1 : max_products_per_geo + 1]:
                    if produced >= max_products_per_geo:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split("\t")
                    direct_link = (parts[2].replace('"', '').strip() if len(parts) > 2 else "") or ""
                    if not direct_link.startswith("http"):
                        for p in parts:
                            p = p.strip()
                            if p.startswith("http"):
                                direct_link = p
                                break
                    if not direct_link:
                        direct_link = "Link Not Found"
                    title = (parts[1].replace('"', '') if len(parts) > 1 else "N/A").strip()
                    master.append({
                        "Country": geo.upper(),
                        "Merchant ID": m_id,
                        "Product Title": title,
                        "Store Link": direct_link,
                        "Audit Status": "Active",
                        "Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    })
                    produced += 1
            except Exception as e:
                logger.warning("PLA %s merchant %s: %s", geo, m_id, e)
                continue
    return master


def write_offers_sheet(
    service: Any,
    spreadsheet_id: str,
    sheet_name: str,
    rows: List[Dict[str, Any]],
) -> None:
    """Write offer rows to sheet. Create sheet if missing (header-only when rows are empty)."""
    headers = ["Country", "Merchant ID", "Product Title", "Store Link", "Audit Status", "Timestamp"]
    data = [headers] + [[r.get(h, "") for h in headers] for r in rows]

    meta = service.get(spreadsheetId=spreadsheet_id, fields="sheets(properties(title))").execute()
    titles = [s["properties"]["title"] for s in meta.get("sheets", [])]
    if sheet_name not in titles:
        service.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]},
        ).execute()

    quoted = sheet_name.replace("'", "''")
    service.values().clear(spreadsheetId=spreadsheet_id, range=f"'{quoted}'!A1:Z10000").execute()
    service.values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{quoted}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": data},
    ).execute()
    logger.info("Wrote %s offers to %s", len(data) - 1, sheet_name)


OFFER_SHEET_HEADERS = [
    "Country",
    "Merchant ID",
    "Product Title",
    "Store Link",
    "Audit Status",
    "Timestamp",
]


def _geo_key_from_offer_country_cell(raw: Any) -> str:
    s = str(raw or "").strip().upper()
    if len(s) < 2:
        return ""
    return s[:2].lower()


def read_offers_sheet_rows(
    service: Any,
    spreadsheet_id: str,
    sheet_name: str,
) -> List[Dict[str, Any]]:
    """Read offer rows from a tab; return [] if missing, empty, or header mismatch."""
    quoted = sheet_name.replace("'", "''")
    try:
        values = service.values().get(
            spreadsheetId=spreadsheet_id,
            range=f"'{quoted}'!A:ZZ",
        ).execute().get("values") or []
    except Exception:
        return []
    if len(values) < 2:
        return []
    header = [str(h or "").strip() for h in values[0]]
    idx = {name: i for i, name in enumerate(header)}
    if not all(h in idx for h in OFFER_SHEET_HEADERS):
        return []
    out: List[Dict[str, Any]] = []
    for r in range(1, len(values)):
        row = values[r]
        out.append({
            h: (row[idx[h]] if idx[h] < len(row) else "")
            for h in OFFER_SHEET_HEADERS
        })
    return out


def merge_offers_replace_geos(
    existing: List[Dict[str, Any]],
    new_rows: List[Dict[str, Any]],
    replace_geos: Set[str],
) -> List[Dict[str, Any]]:
    """
    Drop existing rows whose Country matches any geo in ``replace_geos`` (2-letter, lower),
    then append ``new_rows``. Used when refreshing PLA for a subset of countries without
    wiping the rest of the offers tab.
    """
    rg = {g.lower()[:2] for g in replace_geos if g and len(g.strip()) >= 2}
    if not rg:
        return list(existing) + list(new_rows)
    kept = [
        r
        for r in existing
        if _geo_key_from_offer_country_cell(r.get("Country")) not in rg
    ]
    return kept + list(new_rows)
