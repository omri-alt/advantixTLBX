"""
Yadore API client helpers (feed3).

- POST https://api.yadore.com/v2/deeplink
  Uses `API-Key` header for auth.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence, Union

import requests

from config import YADORE_API_KEY, YADORE_PROJECT_ID
from integrations.monetization_geo import geo_for_yadore

logger = logging.getLogger(__name__)

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
        "market": geo_for_yadore(geo or ""),
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
        "market": geo_for_yadore(geo or ""),
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


def _conversion_detail_click_rows(data: Union[dict, list, Any]) -> List[Dict[str, Any]]:
    """Normalize Yadore conversion/detail JSON to a list of per-click dicts."""
    if isinstance(data, list):
        return [c for c in data if isinstance(c, dict)]
    if not isinstance(data, dict):
        return []

    for key in ("clicks", "conversions", "items", "data", "results"):
        raw = data.get(key)
        if isinstance(raw, list):
            return [c for c in raw if isinstance(c, dict)]
        if isinstance(raw, dict):
            for nk in ("clicks", "items", "rows", "data"):
                nested = raw.get(nk)
                if isinstance(nested, list):
                    return [c for c in nested if isinstance(c, dict)]

    res = data.get("result")
    if isinstance(res, list):
        return [c for c in res if isinstance(c, dict)]
    if isinstance(res, dict):
        for key in ("clicks", "conversions", "items", "data"):
            raw = res.get(key)
            if isinstance(raw, list):
                return [c for c in raw if isinstance(c, dict)]
    return []


def _report_detail_clicks_from_payload(data: Any) -> List[Dict[str, Any]]:
    """``ReportDetailResponse``: ``{ totalClicks, clicks: [...] }`` (same ``clicks`` list shape as conversion endpoints)."""
    return _conversion_detail_click_rows(data)


def _payload_suggests_incomplete_report(payload: Dict[str, Any]) -> bool:
    """Best-effort: Yadore report/status shapes vary; treat explicit false flags as incomplete."""
    for key in ("complete", "ready", "available", "isComplete", "dataComplete"):
        if key in payload and payload[key] is False:
            return True
    nested = payload.get("markets") or payload.get("result") or payload.get("data")
    if isinstance(nested, list):
        for item in nested:
            if isinstance(item, dict):
                for key in ("complete", "ready", "available"):
                    if item.get(key) is False:
                        return True
    return False


def fetch_report_status(
    date: str,
    *,
    api_key: Optional[str] = None,
    base_url: str = YADORE_BASE_URL,
    timeout: int = 60,
) -> Dict[str, Any]:
    """
    GET ``/v2/report/status`` — call before ``report/detail`` when you need finalized numbers.
    """
    token = (api_key or YADORE_API_KEY or "").strip()
    if not token:
        raise YadoreClientError("YADORE_API_KEY is not set")
    endpoint = f"{base_url.rstrip('/')}/v2/report/status"
    headers = {"Accept": "application/json", "API-Key": token}
    params = {"date": date, "format": "json"}
    try:
        r = requests.get(endpoint, headers=headers, params=params, timeout=timeout)
    except requests.RequestException as e:
        raise YadoreClientError(str(e)) from e
    if r.status_code != 200:
        raise YadoreClientError(
            f"report/status HTTP {r.status_code}",
            status_code=r.status_code,
            response_body=(r.text[:800] if r.text else None),
        )
    try:
        data = r.json() if r.text else {}
    except Exception as e:
        raise YadoreClientError(f"report/status JSON error: {e}", response_body=r.text[:500]) from e
    return data if isinstance(data, dict) else {}


def fetch_report_detail_clicks(
    date: str,
    *,
    markets: Optional[Sequence[str]] = None,
    api_key: Optional[str] = None,
    base_url: str = YADORE_BASE_URL,
    timeout: int = 120,
    check_status: bool = True,
) -> List[Dict[str, Any]]:
    """
    GET ``/v2/report/detail`` — click-level report (``clickId``, ``placementId``, ``revenue``, …).

    When ``markets`` is non-empty, one request per market (required for multi-market traffic).
    When empty, a single request is made without a ``market`` query param (single-market accounts).

    Optionally calls ``/v2/report/status`` first and logs a warning if the payload looks incomplete.
    """
    token = (api_key or YADORE_API_KEY or "").strip()
    if not token:
        raise YadoreClientError("YADORE_API_KEY is not set")

    if check_status:
        try:
            st = fetch_report_status(date, api_key=api_key, base_url=base_url, timeout=timeout)
            if _payload_suggests_incomplete_report(st):
                logger.warning(
                    "Yadore report/status for %s may be incomplete; revenue can still change (pull ~3d back per docs). Body keys=%s",
                    date,
                    list(st.keys())[:20],
                )
        except YadoreClientError as e:
            logger.info("Yadore report/status skipped or failed (non-fatal): %s", e)

    endpoint = f"{base_url.rstrip('/')}/v2/report/detail"
    headers = {"Accept": "application/json", "API-Key": token}

    market_list: List[str] = []
    if markets:
        market_list = [geo_for_yadore(str(m)) for m in markets if str(m).strip()]

    def one_request(params: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            r = requests.get(endpoint, headers=headers, params=params, timeout=timeout)
        except requests.RequestException as e:
            raise YadoreClientError(str(e)) from e
        if r.status_code != 200:
            raise YadoreClientError(
                f"report/detail HTTP {r.status_code}",
                status_code=r.status_code,
                response_body=(r.text[:800] if r.text else None),
            )
        try:
            data = r.json() if r.text else {}
        except Exception as e:
            raise YadoreClientError(f"report/detail JSON error: {e}", response_body=r.text[:500]) from e
        return _report_detail_clicks_from_payload(data)

    merged: List[Dict[str, Any]] = []
    seen: set[str] = set()

    if market_list:
        for m in market_list:
            rows = one_request({"date": date, "format": "json", "market": m})
            for row in rows:
                if not isinstance(row, dict):
                    continue
                ck = str(row.get("clickId") or "").strip()
                pl = str(row.get("placementId") or "").strip()
                key = ck if ck else (f"{m}:{pl}" if pl else "")
                if key:
                    if key in seen:
                        continue
                    seen.add(key)
                merged.append(row)
    else:
        rows = one_request({"date": date, "format": "json"})
        for row in rows:
            if not isinstance(row, dict):
                continue
            ck = str(row.get("clickId") or "").strip()
            pl = str(row.get("placementId") or "").strip()
            key = ck or pl
            if key:
                if key in seen:
                    continue
                seen.add(key)
            merged.append(row)

    if not merged:
        logger.info(
            "Yadore report/detail: 0 click rows for date=%s markets=%s",
            date,
            market_list or "(none, single call)",
        )

    return merged


def fetch_conversion_detail(
    date: str,
    *,
    api_key: Optional[str] = None,
    project_id: Optional[str] = None,
    base_url: str = YADORE_BASE_URL,
    timeout: int = 120,
) -> list[dict[str, Any]]:
    """
    GET ``/v2/conversion/detail?date=YYYY-MM-DD&format=json``.

    Sends ``projectId`` when ``YADORE_PROJECT_ID`` / ``project_id`` is set (required for some accounts).
    Accepts several JSON shapes (top-level ``clicks``, nested ``result.clicks``, etc.).
    """
    token = (api_key or YADORE_API_KEY or "").strip()
    if not token:
        raise YadoreClientError("YADORE_API_KEY is not set")

    endpoint = f"{base_url.rstrip('/')}/v2/conversion/detail"
    headers = {
        "Accept": "application/json",
        "API-Key": token,
    }
    params: Dict[str, Any] = {"date": date, "format": "json"}
    pid = (project_id or YADORE_PROJECT_ID or "").strip()
    if pid:
        params["projectId"] = pid

    try:
        r = requests.get(endpoint, headers=headers, params=params, timeout=timeout)
    except requests.RequestException as e:
        raise YadoreClientError(str(e)) from e

    if r.status_code != 200:
        raise YadoreClientError(
            f"conversion/detail HTTP {r.status_code}",
            status_code=r.status_code,
            response_body=(r.text[:800] if r.text else None),
        )

    try:
        data = r.json() if r.text else {}
    except Exception as e:
        raise YadoreClientError(f"conversion/detail JSON error: {e}", response_body=r.text[:500]) from e

    clicks = _conversion_detail_click_rows(data)
    if not clicks and isinstance(data, dict):
        logger.info(
            "Yadore conversion/detail: 0 rows for date=%s projectId=%s; JSON keys=%s",
            date,
            pid or "(none)",
            list(data.keys()),
        )
    return clicks


def fetch_conversion_detail_merchant(
    date_from: str,
    date_to: str,
    *,
    market: Optional[str] = None,
    api_key: Optional[str] = None,
    project_id: Optional[str] = None,
    base_url: str = YADORE_BASE_URL,
    timeout: int = 120,
) -> Dict[str, Any]:
    """
    GET ``/v2/conversion/detail/merchant`` — conversion report grouped by merchant.

    Query: ``from``, ``to`` (yyyy-mm-dd), ``format=json``, optional ``market`` (ISO2),
    optional ``projectId``.
    """
    token = (api_key or YADORE_API_KEY or "").strip()
    if not token:
        raise YadoreClientError("YADORE_API_KEY is not set")

    endpoint = f"{base_url.rstrip('/')}/v2/conversion/detail/merchant"
    headers = {"Accept": "application/json", "API-Key": token}
    params: Dict[str, Any] = {
        "from": (date_from or "").strip()[:10],
        "to": (date_to or "").strip()[:10],
        "format": "json",
    }
    if market and str(market).strip():
        params["market"] = geo_for_yadore(str(market))
    pid = (project_id or YADORE_PROJECT_ID or "").strip()
    if pid:
        params["projectId"] = pid

    try:
        r = requests.get(endpoint, headers=headers, params=params, timeout=timeout)
    except requests.RequestException as e:
        raise YadoreClientError(str(e)) from e

    if r.status_code != 200:
        raise YadoreClientError(
            f"conversion/detail/merchant HTTP {r.status_code}",
            status_code=r.status_code,
            response_body=(r.text[:800] if r.text else None),
        )

    try:
        data = r.json() if r.text else {}
    except Exception as e:
        raise YadoreClientError(f"conversion/detail/merchant JSON error: {e}", response_body=r.text[:500]) from e

    return data if isinstance(data, dict) else {}


def parse_conversion_detail_merchant_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Normalize ``salesByMerchant`` (or nested ``result``) to a flat list of dicts with keys:
    ``market``, ``merchant_id``, ``merchant_name``, ``clicks``, ``sales``, ``merchant_url`` (optional).
    """
    root = payload
    if isinstance(payload.get("result"), dict):
        inner = payload["result"]
        if isinstance(inner.get("salesByMerchant"), list):
            root = inner

    arr = root.get("salesByMerchant")
    if not isinstance(arr, list):
        return []

    out: List[Dict[str, Any]] = []
    for item in arr:
        if not isinstance(item, dict):
            continue
        mkt = str(item.get("market") or "").strip().lower()[:2]
        merch = item.get("merchant") if isinstance(item.get("merchant"), dict) else {}
        mid = str((merch or {}).get("id") or item.get("merchantId") or "").strip()
        mname = str((merch or {}).get("name") or item.get("merchantName") or "").strip()
        try:
            clicks = int(item.get("clicks") or 0)
        except (TypeError, ValueError):
            clicks = 0
        try:
            sales = int(item.get("sales") or item.get("saleCount") or 0)
        except (TypeError, ValueError):
            sales = 0
        murl = str((merch or {}).get("url") or (merch or {}).get("website") or item.get("url") or "").strip()
        out.append(
            {
                "market": mkt,
                "merchant_id": mid,
                "merchant_name": mname,
                "clicks": clicks,
                "sales": sales,
                "merchant_url": murl,
            }
        )
    return out

