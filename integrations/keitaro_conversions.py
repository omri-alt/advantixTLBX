"""
Keitaro conversion log (``POST admin_api/v1/conversions/log``).

Used to see which ``sub_id`` values already received sale postbacks (``SaleOur`` / ``LateSale``)
with a given ``params.payout``.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qs

from integrations.keitaro import KeitaroClient, KeitaroClientError

logger = logging.getLogger(__name__)

_LOG_COLUMNS = [
    "sub_id",
    "sub_id_6",
    "campaign",
    "offer",
    "datetime",
    "status",
    "conversion_type",
    "params",
]
_DEFAULT_PAGE_SIZE = 5000
_SALE_POSTBACK_STATUSES = frozenset({"SaleOur", "LateSale"})


def normalize_payout(value: Any) -> str:
    """Normalize monetary payout for comparison (handles commas, trailing zeros)."""
    s = str(value if value is not None else "0").strip().replace(",", ".")
    if not s:
        return "0"
    try:
        n = float(s)
        if abs(n - round(n)) < 1e-9:
            return str(int(round(n)))
        out = f"{n:.6f}".rstrip("0").rstrip(".")
        return out or "0"
    except ValueError:
        return s


def payout_from_keitaro_params(params: Any) -> Optional[str]:
    """Extract ``payout`` from Keitaro log ``params`` (dict, JSON string, or query string)."""
    if params is None:
        return None
    if isinstance(params, dict):
        for key in ("payout", "Payout", "revenue"):
            if key in params and params[key] is not None and str(params[key]).strip():
                return normalize_payout(params[key])
        return None
    if isinstance(params, str):
        s = params.strip()
        if not s:
            return None
        if s.startswith("{"):
            try:
                parsed = json.loads(s)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, dict):
                return payout_from_keitaro_params(parsed)
        qs = parse_qs(s, keep_blank_values=True)
        if "payout" in qs and qs["payout"]:
            return normalize_payout(qs["payout"][0])
        for part in s.split("&"):
            if part.lower().startswith("payout="):
                return normalize_payout(part.split("=", 1)[-1])
    return None


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
    columns: Optional[Sequence[str]] = None,
) -> Iterable[Dict[str, Any]]:
    """
    Paginate conversion log rows in ``[date_from, date_to]`` (inclusive, UTC day bounds).

    ``status``: when set, only rows with that conversion status (e.g. ``LateSale``, ``SaleOur``).
    ``columns``: override default log columns (e.g. include ``sub_id_5`` for SK WL sync).
    """
    url = client._api_path("conversions/log")
    d0 = date_from.isoformat()
    d1 = date_to.isoformat()
    cols = list(columns) if columns else list(_LOG_COLUMNS)
    offset = 0
    while True:
        body: Dict[str, Any] = {
            "range": {"from": f"{d0} 00:00:00", "to": f"{d1} 23:59:59"},
            "columns": cols,
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


def collect_sale_postback_keys(
    *,
    date_from: date,
    date_to: date,
    client: Optional[KeitaroClient] = None,
    statuses: Optional[Iterable[str]] = None,
) -> Set[Tuple[str, str]]:
    """
    Set of ``(sub_id, normalized_payout)`` for sale postbacks already in Keitaro.

    Only ``SaleOur`` and ``LateSale`` rows with a parseable ``params.payout`` are included.
    """
    if client is None:
        client = KeitaroClient()
    want = tuple(statuses or _SALE_POSTBACK_STATUSES)
    keys: Set[Tuple[str, str]] = set()
    for st in want:
        st_clean = (st or "").strip()
        if not st_clean:
            continue
        try:
            for row in iter_conversion_log(client, date_from=date_from, date_to=date_to, status=st_clean):
                sid = str(row.get("sub_id") or "").strip()
                po = payout_from_keitaro_params(row.get("params"))
                if sid and po is not None:
                    keys.add((sid, po))
        except KeitaroClientError as e:
            logger.warning("Keitaro sale postback scan status=%s failed: %s", st_clean, e)
    return keys


def has_matching_sale_postback(
    sub_id: str,
    payout: str,
    known: Set[Tuple[str, str]],
) -> bool:
    """True if Keitaro already logged a sale postback for this sub_id + payout."""
    sid = (sub_id or "").strip()
    if not sid:
        return False
    po = normalize_payout(payout)
    return (sid, po) in known


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
