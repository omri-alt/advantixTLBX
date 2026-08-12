"""
Trillion Direct API helpers.

Auth: ``Authorization: Bearer <KEYTR>``.
Endpoint: ``https://www.trillion.com/api.html`` with query params.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import requests

TRILLION_API_URL = "https://www.trillion.com/api.html"


class TrillionClientError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None, response_body: Optional[str] = None):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(message)


def _headers(api_key: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}


def _request_json(api_key: str, *, params: Dict[str, Any], method: str = "POST") -> Dict[str, Any]:
    fn = requests.post if method.upper() == "POST" else requests.get
    r = fn(TRILLION_API_URL, params=params, headers=_headers(api_key), timeout=45)
    if r.status_code != 200:
        raise TrillionClientError(
            f"Trillion API error: {r.status_code}",
            status_code=r.status_code,
            response_body=r.text[:500] if r.text else None,
        )
    try:
        data = r.json() if r.text else {}
    except Exception:
        raise TrillionClientError("Trillion API returned non-JSON", status_code=r.status_code)
    if not isinstance(data, dict):
        raise TrillionClientError("Trillion API response is not an object", status_code=r.status_code)
    errs = data.get("errors")
    if isinstance(errs, list) and errs:
        first = errs[0] if isinstance(errs[0], dict) else {}
        msg = str(first.get("error") or "Trillion API returned errors[]")
        code = str(first.get("code") or "").strip()
        raise TrillionClientError(
            f"{msg}{f' (code {code})' if code else ''}",
            status_code=r.status_code,
            response_body=r.text[:500] if r.text else None,
        )
    return data


def list_campaigns(
    api_key: str,
    *,
    folder: str = "",
    campaign: str = "",
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"mode": "list_campaigns", "type": "json"}
    if folder:
        params["folder"] = folder
    if campaign:
        params["campaign"] = campaign
    if limit is not None:
        params["limit"] = int(limit)
    if offset is not None and limit is not None:
        params["offset"] = int(offset)
    data = _request_json(api_key, params=params, method="POST")
    rows = data.get("results")
    if not isinstance(rows, list):
        return []
    return [x for x in rows if isinstance(x, dict)]


def update_ron_active(api_key: str, *, ron: str, active: bool) -> Dict[str, Any]:
    return update_ron(api_key, ron=ron, active=active)


def update_ron(
    api_key: str,
    *,
    ron: str,
    active: Optional[bool] = None,
    my_bid: Optional[float] = None,
    daily_limit: Optional[float] = None,
) -> Dict[str, Any]:
    """Update RON campaign settings (``update_ron`` API mode)."""
    params: Dict[str, Any] = {
        "mode": "update_ron",
        "type": "json",
        "ron": ron,
    }
    if active is not None:
        params["active"] = "1" if active else "0"
    if my_bid is not None:
        params["my_bid"] = f"{float(my_bid):.4f}".rstrip("0").rstrip(".")
    if daily_limit is not None:
        params["daily_limit"] = f"{float(daily_limit):.2f}"
    if active is None and my_bid is None and daily_limit is None:
        raise ValueError("update_ron requires at least one of active, my_bid, daily_limit")
    return _request_json(api_key, params=params, method="POST")


def fetch_report(
    api_key: str,
    *,
    period: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    groupby: Optional[str] = None,
    campaign: Optional[str] = None,
    folder: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """``mode=report`` — returns ``results`` rows (may be empty while processing)."""
    params: Dict[str, Any] = {"mode": "report", "type": "json"}
    if period:
        params["period"] = period
    if from_date:
        params["from_date"] = from_date
    if to_date:
        params["to_date"] = to_date
    if groupby:
        params["groupby"] = groupby
    if campaign:
        params["campaign"] = campaign
    if folder:
        params["folder"] = folder
    data = _request_json(api_key, params=params, method="POST")
    rows = data.get("results")
    if not isinstance(rows, list):
        return []
    return [x for x in rows if isinstance(x, dict)]


def parse_trillion_money(raw: Any) -> Optional[float]:
    """Parse Trillion report/list fields like ``11.560`` or ``$50.00``."""
    if raw is None or raw == "":
        return None
    s = str(raw).strip().replace("$", "").replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None
