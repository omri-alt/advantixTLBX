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
    params = {
        "mode": "update_ron",
        "type": "json",
        "ron": ron,
        "active": "1" if active else "0",
    }
    return _request_json(api_key, params=params, method="POST")
