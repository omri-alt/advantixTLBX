"""
Keitaro conversion log (``POST admin_api/v1/conversions/log``).

Used to see which ``sub_id`` values already received ``SaleOur`` vs ``LateSale`` postbacks.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from integrations.keitaro import KeitaroClient, KeitaroClientError

logger = logging.getLogger(__name__)

_LOG_COLUMNS = ["sub_id", "status", "revenue", "datetime"]
_DEFAULT_PAGE_SIZE = 5000


def _status_filter_expression(status: str) -> List[Dict[str, str]]:
    st = (status or "").strip()
    if not st:
        return []
    return [{"name": "status", "operator": "EQUALS", "expression": st}]


def iter_conversion_log(
    client: KeitaroClient,
    *,
    date_from: date,
    date_to: date,
    status: Optional[str] = None,
    page_size: int = _DEFAULT_PAGE_SIZE,
) -> Iterable[Dict[str, Any]]:
    """
    Paginate conversion log rows in ``[date_from, date_to]`` (inclusive, UTC day bounds).

    ``status``: when set, only rows with that conversion status (e.g. ``LateSale``, ``SaleOur``).
    """
    url = client._api_path("conversions/log")
    d0 = date_from.isoformat()
    d1 = date_to.isoformat()
    offset = 0
    while True:
        body: Dict[str, Any] = {
            "range": {"from": f"{d0} 00:00:00", "to": f"{d1} 23:59:59"},
            "columns": list(_LOG_COLUMNS),
            "limit": max(1, min(int(page_size), 10000)),
            "offset": offset,
        }
        filt = _status_filter_expression(status or "")
        if filt:
            body["filters"] = filt
        try:
            resp = client._session.post(url, json=body, timeout=120)
        except Exception as e:
            raise KeitaroClientError(str(e)) from e
        if not resp.ok:
            raise KeitaroClientError(
                f"conversions/log HTTP {resp.status_code}",
                resp.status_code,
                (resp.text or "")[:500],
            )
        try:
            data = resp.json()
        except Exception as e:
            raise KeitaroClientError(f"conversions/log invalid JSON: {e}", resp.status_code) from e
        rows = data.get("rows") if isinstance(data, dict) else None
        if not isinstance(rows, list):
            rows = []
        if not rows:
            break
        for row in rows:
            if isinstance(row, dict):
                yield row
        total = data.get("total") if isinstance(data, dict) else None
        offset += len(rows)
        if len(rows) < body["limit"]:
            break
        if isinstance(total, int) and offset >= total:
            break
        if offset > 500_000:
            logger.warning("Keitaro conversions/log pagination cap at offset=%s", offset)
            break


def collect_subids_by_status(
    client: KeitaroClient,
    *,
    date_from: date,
    date_to: date,
    statuses: Iterable[str],
) -> Dict[str, Set[str]]:
    """Map each requested status string to the set of ``sub_id`` seen in the log."""
    want = {(s or "").strip() for s in statuses if (s or "").strip()}
    out: Dict[str, Set[str]] = {s: set() for s in want}
    if not want:
        return out
    for st in sorted(want):
        try:
            for row in iter_conversion_log(client, date_from=date_from, date_to=date_to, status=st):
                sid = str(row.get("sub_id") or "").strip()
                if sid:
                    out[st].add(sid)
        except KeitaroClientError as e:
            logger.warning("Keitaro conversions/log status=%s failed: %s", st, e)
    return out


def collect_keitaro_conversion_subids_by_status(
    *,
    lookback_days: int = 45,
    statuses: Optional[Iterable[str]] = None,
    client: Optional[KeitaroClient] = None,
) -> Dict[str, Set[str]]:
    """Recent conversion log sub_ids grouped by status (e.g. ``SaleOur``, ``LateSale``)."""
    if client is None:
        client = KeitaroClient()
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=max(1, lookback_days))
    want = tuple(statuses or ("SaleOur", "LateSale"))
    return collect_subids_by_status(client, date_from=start, date_to=today, statuses=want)


def collect_late_sale_dedup_subids(
    client: Optional[KeitaroClient] = None,
    *,
    lookback_days: int = 45,
    late_status: str = "LateSale",
) -> Set[str]:
    """Sub IDs that already have a LateSale conversion in Keitaro (recent window)."""
    if client is None:
        client = KeitaroClient()
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=max(1, lookback_days))
    try:
        by_st = collect_subids_by_status(
            client, date_from=start, date_to=today, statuses=[late_status]
        )
        return set(by_st.get(late_status) or set())
    except KeitaroClientError as e:
        logger.warning("Keitaro LateSale dedup unavailable: %s", e)
        return set()
