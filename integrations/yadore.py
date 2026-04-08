"""
Yadore API client helpers (feed3).

- POST https://api.yadore.com/v2/deeplink
  Uses `API-Key` header for auth.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import requests

from config import YADORE_API_KEY, YADORE_PROJECT_ID

YADORE_BASE_URL = "https://api.yadore.com"


class YadoreClientError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None, response_body: Optional[str] = None):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(message)


def deeplink(
    url: str,
    geo: str,
    *,
    placement_id: str = "WAF4IibbRqGG",
    is_couponing: bool = False,
    api_key: Optional[str] = None,
    base_url: str = YADORE_BASE_URL,
) -> Dict[str, Any]:
    token = (api_key or YADORE_API_KEY or "").strip()
    if not token:
        raise YadoreClientError("YADORE_API_KEY is not set")

    endpoint = f"{base_url.rstrip('/')}/v2/deeplink"
    headers = {
        "Content-Type": "application/json",
        "accept": "application/json",
        "API-Key": token,
    }
    payload = {
        "market": (geo or "").strip().lower(),
        "placementId": placement_id,
        "isCouponing": bool(is_couponing),
        "urls": [{"url": url}],
    }
    try:
        r = requests.post(endpoint, headers=headers, json=payload, timeout=30)
    except requests.RequestException as e:
        raise YadoreClientError(str(e)) from e

    http = r.status_code
    data = r.json() if r.text else {}
    if http != 200:
        raise YadoreClientError(
            f"Yadore API error: {http}",
            status_code=http,
            response_body=(r.text[:500] if r.text else None),
        )

    root = data.get("result") if isinstance(data, dict) and isinstance(data.get("result"), dict) else data
    first = (root.get("deeplinks") or [None])[0] or {}
    est = first.get("estimatedCpc")
    est_amount = (est.get("amount") if isinstance(est, dict) else est) if est is not None else None
    est_currency = (est.get("currency") if isinstance(est, dict) else None) if est is not None else None
    logo_url = (((first.get("merchant") or {}).get("logo") or {}).get("url")) or ""

    return {
        "http": http,
        "root_found": root.get("found"),
        "root_total": root.get("total"),
        "found": bool(first.get("found")),
        "echo_url": first.get("url") or "",
        "clickUrl": first.get("clickUrl") or first.get("deeplink") or "",
        "estimatedCpc_amount": est_amount,
        "estimatedCpc_currency": est_currency,
        "logoUrl": logo_url,
        "raw": data,
    }


def direct_redirect_probe(
    url: str,
    geo: str,
    *,
    placement_id: str = "WAF4IibbRqGG",
    is_couponing: bool = False,
    api_key: Optional[str] = None,
    project_id: Optional[str] = None,
    base_url: str = YADORE_BASE_URL,
) -> Dict[str, Any]:
    token = (api_key or YADORE_API_KEY or "").strip()
    if not token:
        raise YadoreClientError("YADORE_API_KEY is not set")
    pid = (project_id or YADORE_PROJECT_ID or "").strip() or None

    endpoint = f"{base_url.rstrip('/')}/v2/d"
    params = {
        "url": url,
        "market": (geo or "").strip().lower(),
        "placementId": placement_id,
        "isCouponing": "true" if is_couponing else "false",
    }
    if pid:
        params["projectId"] = pid
    headers = {"API-Key": token}
    r = requests.get(endpoint, headers=headers, params=params, timeout=30, allow_redirects=False)
    loc = r.headers.get("Location") or ""
    monetized = r.status_code in (301, 302, 307, 308) and bool(loc)
    return {
        "http": r.status_code,
        "monetized": monetized,
        "location": loc,
        "body_snippet": (r.text or "")[:300],
        "used_projectId": bool(pid),
    }

