"""Effinity publisher conversions API (``GET .../publisher/conversions``)."""
from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import requests

from config import EFFINITY_API_BASE, EFFINITY_API_KEY

logger = logging.getLogger(__name__)

_HTTP_TIMEOUT = 45
_DEFAULT_PER_PAGE = 1000
_CANCELLED = frozenset({"cancelled", "canceled"})


class EffinityClientError(RuntimeError):
    pass


def _api_key() -> str:
    key = (EFFINITY_API_KEY or "").strip()
    if not key:
        raise EffinityClientError("KEYEFFINITY / EFFINITY_API_KEY is not set")
    return key


def _conversion_sub_id(row: Dict[str, Any]) -> str:
    for field in ("effiId", "effiId2", "reference", "sessionId"):
        raw = row.get(field)
        if raw is None:
            continue
        s = str(raw).strip()
        if s:
            return s
    return ""


def _conversion_commission(row: Dict[str, Any]) -> str:
    raw = row.get("commissionAmount")
    if raw is None:
        return "0"
    try:
        n = float(raw)
        if abs(n - round(n)) < 1e-9:
            return str(int(round(n)))
        return f"{n:.6f}".rstrip("0").rstrip(".") or "0"
    except (TypeError, ValueError):
        return str(raw).strip() or "0"


def _include_conversion(row: Dict[str, Any], *, sales_only: bool) -> bool:
    st = str(row.get("status") or "").strip().lower()
    if st in _CANCELLED:
        return False
    if sales_only:
        ctype = str(row.get("conversionType") or "").strip().lower()
        if ctype and ctype not in ("sale", "sales"):
            return False
    sid = _conversion_sub_id(row)
    if not sid:
        return False
    return True


def fetch_conversions_paged(
    start: date,
    end: date,
    *,
    date_type: str = "conversionDate",
    conversion_type: str = "all",
    per_page: int = _DEFAULT_PER_PAGE,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Paginate ``GET {base}/{apikey}/publisher/conversions``.

    Returns ``(rows, error)`` — error is set only when no rows were retrieved.
    """
    if end < start:
        return [], None
    key = _api_key()
    url = f"{EFFINITY_API_BASE}/{key}/publisher/conversions"
    start_s = start.isoformat()
    end_s = end.isoformat()
    out: List[Dict[str, Any]] = []
    page = 1
    last_err: Optional[str] = None

    while page <= 500:
        params = {
            "dateType": date_type,
            "conversionType": conversion_type,
            "start": start_s,
            "end": end_s,
            "page": page,
            "perPage": max(1, min(int(per_page), 1000)),
        }
        try:
            r = requests.get(url, params=params, timeout=_HTTP_TIMEOUT)
        except requests.RequestException as e:
            last_err = str(e)
            break
        if r.status_code == 429:
            time.sleep(2.0)
            continue
        if r.status_code != 200:
            last_err = f"HTTP {r.status_code}: {(r.text or '')[:200]}"
            break
        try:
            data = r.json()
        except Exception as e:
            last_err = str(e)
            break
        if not isinstance(data, dict):
            last_err = "invalid JSON response"
            break
        items = data.get("conversions")
        if not isinstance(items, list):
            last_err = "missing conversions array"
            break
        for it in items:
            if isinstance(it, dict):
                out.append(it)
        pag = data.get("pagination") if isinstance(data.get("pagination"), dict) else {}
        current = int(pag.get("currentPage") or page)
        last_page = int(pag.get("lastPage") or current)
        if current >= last_page or not items:
            break
        page = current + 1
        time.sleep(0.05)

    if not out and last_err:
        return [], last_err
    return out, None


def fetch_mtd_sale_conversions(
    month_start: date,
    end: date,
    *,
    sales_only: bool = True,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """MTD sales/leads from Effinity; deduped by ``conversionId``."""
    raw, err = fetch_conversions_paged(month_start, end, conversion_type="all")
    if err and not raw:
        return [], err
    seen_ids: set[str] = set()
    out: List[Dict[str, Any]] = []
    for row in raw:
        if not _include_conversion(row, sales_only=sales_only):
            continue
        cid = str(row.get("conversionId") or "").strip()
        if cid:
            if cid in seen_ids:
                continue
            seen_ids.add(cid)
        out.append(row)
    return out, err
