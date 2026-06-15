"""
Yadore API client helpers (feed3).

- POST https://api.yadore.com/v2/deeplink
  Uses `API-Key` header for auth.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence, Union
from urllib.parse import quote

import requests

from config import BLEND_YADORE_OFFER_USE_SUB_MACROS, YADORE_API_KEY, YADORE_IS_COUPONING, YADORE_PROJECT_ID
from integrations.monetization_geo import geo_for_yadore

logger = logging.getLogger(__name__)

YADORE_BASE_URL = "https://api.yadore.com"
YADORE_KEITARO_RAIN_SHELL = "https://shopli.city/rainotest?rain="
YADORE_DEEPLINK_PROJECT_FALLBACK = "WAF4IibbRqGG"


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
    is_smartlink = first.get("isSmartlink")
    if is_smartlink is None and isinstance(first.get("merchant"), dict):
        is_smartlink = (first.get("merchant") or {}).get("isSmartlink")

    return {
        "http": http,
        "root_found": root.get("found"),
        "root_total": root.get("total"),
        "found": bool(first.get("found")) or bool(str(first.get("clickUrl") or first.get("deeplink") or "").strip()),
        "echo_url": first.get("url") or "",
        "clickUrl": first.get("clickUrl") or first.get("deeplink") or "",
        "estimatedCpc_amount": est_amount,
        "estimatedCpc_currency": est_currency,
        "logoUrl": logo_url,
        "isSmartlink": is_smartlink,
        "raw": data,
    }


def deeplink_batch(
    urls: Sequence[str],
    geo: str,
    *,
    placement_id: str = "WAF4IibbRqGG",
    is_couponing: bool = False,
    api_key: Optional[str] = None,
    base_url: str = YADORE_BASE_URL,
) -> List[Dict[str, Any]]:
    """POST /v2/deeplink with up to 20 URLs; returns normalized per-URL rows."""
    batch = [u.strip() for u in urls if (u or "").strip()][:20]
    if not batch:
        return []
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
        "urls": [{"url": u} for u in batch],
    }
    try:
        r = requests.post(endpoint, headers=headers, json=payload, timeout=45)
    except requests.RequestException as e:
        raise YadoreClientError(str(e)) from e

    if r.status_code != 200:
        raise YadoreClientError(
            f"Yadore API error: {r.status_code}",
            status_code=r.status_code,
            response_body=(r.text[:500] if r.text else None),
        )

    data = r.json() if r.text else {}
    root = data.get("result") if isinstance(data, dict) and isinstance(data.get("result"), dict) else data
    out: List[Dict[str, Any]] = []
    for item in root.get("deeplinks") or []:
        if not isinstance(item, dict):
            continue
        est = item.get("estimatedCpc")
        est_amount = (est.get("amount") if isinstance(est, dict) else est) if est is not None else None
        est_currency = (est.get("currency") if isinstance(est, dict) else None) if est is not None else None
        out.append(
            {
                "found": bool(item.get("found")) or bool(str(item.get("clickUrl") or item.get("deeplink") or "").strip()),
                "echo_url": item.get("url") or "",
                "clickUrl": item.get("clickUrl") or item.get("deeplink") or "",
                "estimatedCpc_amount": est_amount,
                "estimatedCpc_currency": est_currency,
                "isSmartlink": item.get("isSmartlink"),
                "raw": item,
            }
        )
    return out


def fetch_deeplink_merchants(
    market: str,
    *,
    api_key: Optional[str] = None,
    base_url: str = YADORE_BASE_URL,
    timeout: int = 120,
) -> List[Dict[str, Any]]:
    """GET /v2/deeplink/merchant — active deeplink + smartlink merchants for a market."""
    token = (api_key or YADORE_API_KEY or "").strip()
    if not token:
        raise YadoreClientError("YADORE_API_KEY is not set")
    endpoint = f"{base_url.rstrip('/')}/v2/deeplink/merchant"
    headers = {"Accept": "application/json", "API-Key": token}
    params = {"market": geo_for_yadore(market or "")}
    try:
        r = requests.get(endpoint, headers=headers, params=params, timeout=timeout)
    except requests.RequestException as e:
        raise YadoreClientError(str(e)) from e
    if r.status_code != 200:
        raise YadoreClientError(
            f"deeplink/merchant HTTP {r.status_code}",
            status_code=r.status_code,
            response_body=(r.text[:800] if r.text else None),
        )
    data = r.json() if r.text else {}
    arr = data.get("merchants")
    if not isinstance(arr, list):
        inner = data.get("result") if isinstance(data.get("result"), dict) else {}
        arr = inner.get("merchants") if isinstance(inner.get("merchants"), list) else []
    return [m for m in arr if isinstance(m, dict)]


def _merchant_url_https(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return u
    if not u.lower().startswith(("http://", "https://")):
        u = "https://" + u.lstrip("/")
    return u


def build_yadore_keitaro_payload(
    geo: str,
    merchant_url: str,
    *,
    project_id: Optional[str] = None,
    use_sub_macros: Optional[bool] = None,
    is_couponing: Optional[bool] = None,
) -> str:
    """
    Keitaro offer URL for Yadore Direct Redirect (feed3).

    ``shopli.city/rainotest`` → ``api.yadore.com/v2/d`` with ``placementId={subid}``.
    Default embeds merchant homepage + market from notes; optional sub macros mode
    uses ``url={sub_id_3}&market={sub_id_2}`` when ``BLEND_YADORE_OFFER_USE_SUB_MACROS=1``.
    """
    pid = (project_id or YADORE_PROJECT_ID or "").strip() or YADORE_DEEPLINK_PROJECT_FALLBACK
    pid_q = quote(str(pid), safe="")
    sub_mode = BLEND_YADORE_OFFER_USE_SUB_MACROS if use_sub_macros is None else bool(use_sub_macros)
    couponing = YADORE_IS_COUPONING if is_couponing is None else bool(is_couponing)
    coupon_q = "true" if couponing else "false"

    if sub_mode:
        inner = (
            "https://api.yadore.com/v2/d"
            f"?url={{sub_id_3}}&market={{sub_id_2}}"
            f"&placementId={{subid}}&projectId={pid_q}&isCouponing={coupon_q}"
        )
        return YADORE_KEITARO_RAIN_SHELL + quote(inner, safe=":/?&={}")

    g = (geo or "").strip().lower()[:2]
    if g == "gb":
        g = "uk"
    if len(g) != 2:
        return ""
    market = geo_for_yadore(g)
    m_enc = quote(_merchant_url_https(merchant_url), safe="")
    inner = (
        "https://api.yadore.com/v2/d"
        f"?url={m_enc}&market={quote(str(market), safe='')}"
        f"&placementId={{subid}}&projectId={pid_q}&isCouponing={coupon_q}"
    )
    return YADORE_KEITARO_RAIN_SHELL + quote(inner, safe=":/?&={}%")


def _yadore_host(s: str) -> str:
    from urllib.parse import urlparse

    t = (s or "").strip().lower()
    if not t:
        return ""
    if "://" in t:
        t = urlparse(t).netloc or t.split("://", 1)[-1]
    if t.startswith("www."):
        t = t[4:]
    return t.split("/")[0].split("?")[0]


def _yadore_registrable_domain(host: str) -> str:
    h = _yadore_host(host)
    parts = h.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return h


def _catalog_host_match(probe_host: str, catalog_name: str) -> bool:
    """Match campaign URL host to Yadore catalog ``name`` (usually bare domain)."""
    ph = _yadore_host(probe_host)
    ch = _yadore_host(catalog_name)
    if not ph or not ch:
        return False
    if ph == ch:
        return True
    if ph.endswith("." + ch):
        return True
    if _yadore_registrable_domain(ph) == ch:
        return True
    if _yadore_registrable_domain(ph) == _yadore_registrable_domain(ch):
        return True
    return False


def find_catalog_merchant(
    rows: Optional[List[Dict[str, Any]]],
    *,
    host: str,
    merchant_name: str = "",
) -> Optional[Dict[str, Any]]:
    probes: List[str] = []
    if host:
        probes.append(host)
    name_host = _yadore_host(merchant_name)
    if name_host and name_host not in probes:
        probes.append(name_host)
    for row in rows or []:
        catalog_name = str(row.get("name") or "")
        if not catalog_name:
            continue
        for probe in probes:
            if _catalog_host_match(probe, catalog_name):
                return row
    return None


def merchant_monetization_check(
    merchant_url: str,
    country_iso2: str,
    *,
    merchant_name: str = "",
    placement_id: str = "WAF4IibbRqGG",
    api_key: Optional[str] = None,
    deeplink_merchants: Optional[List[Dict[str, Any]]] = None,
    is_couponing: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Yadore monetization for checkmon / potential sheets.

    Coupon-inclusive account: one ``POST /v2/deeplink`` with ``isCouponing=true``
    (``YADORE_IS_COUPONING`` / ``is_couponing`` override).

    **deeplink** — specific landing URL; **smartlink** — ``isSmartlink`` merchant.
    Catalog fallback when homepage probes return no ``clickUrl``.
    """
    coupon_flag = YADORE_IS_COUPONING if is_couponing is None else bool(is_couponing)
    host = _yadore_host(merchant_url) or _yadore_host(merchant_name)
    market = geo_for_yadore(country_iso2 or "")

    rows = deeplink_merchants
    if rows is None:
        try:
            rows = fetch_deeplink_merchants(market, api_key=api_key)
        except YadoreClientError:
            rows = []
    catalog_row = find_catalog_merchant(rows, host=host, merchant_name=merchant_name)

    urls: List[str] = []
    seen: set[str] = set()

    def _add_url(candidate: str) -> None:
        c = (candidate or "").strip()
        if not c:
            return
        if not c.lower().startswith("http"):
            c = f"https://{c.lstrip('/')}"
        if c not in seen:
            seen.add(c)
            urls.append(c)

    if catalog_row:
        cn = str(catalog_row.get("name") or "").strip()
        if cn:
            _add_url(f"https://{cn}")
            _add_url(f"https://www.{cn}")
    for candidate in (merchant_url, merchant_name, f"https://{host}" if host else "", f"https://www.{host}" if host else ""):
        _add_url(candidate)

    click_url = ""
    probe_echo_url = ""
    ecpc = ""
    err = ""
    api_smartlink: Optional[bool] = None
    deeplink_link_found = False

    for url in urls:
        try:
            d = deeplink(
                url,
                country_iso2,
                placement_id=placement_id,
                is_couponing=coupon_flag,
                api_key=api_key,
            )
        except YadoreClientError as e:
            err = str(e)[:200]
            continue
        got_click = bool(str(d.get("clickUrl") or "").strip())
        if bool(d.get("found")) or got_click:
            deeplink_link_found = True
            click_url = click_url or str(d.get("clickUrl") or "")
            probe_echo_url = probe_echo_url or str(d.get("echo_url") or url)
            ecpc = ecpc or str(d.get("estimatedCpc_amount") or "")
            if d.get("isSmartlink") is not None:
                api_smartlink = bool(d.get("isSmartlink"))
            break

    catalog_smartlink = bool(catalog_row.get("isSmartlink")) if catalog_row else False
    smartlink_found = catalog_smartlink or bool(api_smartlink)

    mode = "none"
    note = err or "not_in_catalog"
    found = deeplink_link_found or bool(catalog_row)

    if deeplink_link_found:
        mode = "smartlink" if smartlink_found else "deeplink"
        note = "deeplink_api"
    elif catalog_row:
        found = True
        if catalog_smartlink:
            mode = "smartlink_catalog"
            smartlink_found = True
            note = "active smartlink merchant in /v2/deeplink/merchant (URL probe returned no clickUrl)"
        else:
            mode = "deeplink_catalog"
            deeplink_link_found = True
            note = "active deeplink merchant in catalog (URL probe returned no clickUrl)"
        est = catalog_row.get("estimatedCpc")
        if isinstance(est, dict) and est.get("amount"):
            ecpc = str(est.get("amount") or "")

    probe_url = probe_echo_url or (urls[0] if urls else _merchant_url_https(merchant_url))
    if catalog_row and not probe_echo_url:
        cn = str(catalog_row.get("name") or "").strip()
        if cn:
            probe_url = _merchant_url_https(f"https://{cn}")

    keitaro_offer_url = build_yadore_keitaro_payload(country_iso2, probe_url, is_couponing=coupon_flag) if found else ""

    return {
        "found": found,
        "mode": mode,
        "note": note,
        "deeplink_link_found": deeplink_link_found,
        "smartlink_found": smartlink_found,
        "non_coupon_found": deeplink_link_found and not smartlink_found,
        "coupon_found": smartlink_found,
        "clickUrl": click_url,
        "estimated_cpc": ecpc,
        "is_smartlink": smartlink_found,
        "has_smartlink_homepage": bool(catalog_row.get("hasSmartlinkHomepage")) if catalog_row else False,
        "catalog_merchant_id": str((catalog_row or {}).get("id") or ""),
        "probe_host": host,
        "probe_url": probe_url,
        "keitaro_offer_url": keitaro_offer_url,
        "is_couponing": coupon_flag,
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
    market: Optional[str] = None,
    api_key: Optional[str] = None,
    project_id: Optional[str] = None,
    base_url: str = YADORE_BASE_URL,
    timeout: int = 120,
) -> list[dict[str, Any]]:
    """
    GET ``/v2/conversion/detail?date=YYYY-MM-DD&format=json`` (optional ``market``).

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
            "Yadore conversion/detail: 0 rows for date=%s market=%s projectId=%s; JSON keys=%s",
            date,
            params.get("market") or "(all)",
            pid or "(none)",
            list(data.keys()),
        )
    return clicks


def fetch_conversion_detail_clicks(
    date: str,
    *,
    markets: Optional[Sequence[str]] = None,
    api_key: Optional[str] = None,
    project_id: Optional[str] = None,
    base_url: str = YADORE_BASE_URL,
    timeout: int = 120,
) -> List[Dict[str, Any]]:
    """
    Conversion report per click (paid conversions). One API call per market when ``markets`` is set.

    Yadore docs: use ``/v2/conversion/detail`` for sales; ``/v2/report/detail`` is click/CPC revenue only.
    """
    market_list: List[str] = []
    if markets:
        market_list = [geo_for_yadore(str(m)) for m in markets if str(m).strip()]

    merged: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def _merge(rows: List[Dict[str, Any]], mkt: str) -> None:
        for row in rows:
            if not isinstance(row, dict):
                continue
            pl = str(row.get("placementId") or row.get("placement_id") or "").strip()
            ck = str(row.get("clickId") or row.get("click_id") or "").strip()
            key = pl or ck or f"{mkt}:{len(merged)}"
            if key in seen:
                continue
            seen.add(key)
            merged.append(row)

    if market_list:
        for m in market_list:
            rows = fetch_conversion_detail(
                date,
                market=m,
                api_key=api_key,
                project_id=project_id,
                base_url=base_url,
                timeout=timeout,
            )
            _merge(rows, m)
    else:
        _merge(
            fetch_conversion_detail(
                date,
                api_key=api_key,
                project_id=project_id,
                base_url=base_url,
                timeout=timeout,
            ),
            "",
        )

    return merged


def fetch_conversion_general(
    date_from: str,
    date_to: str,
    *,
    api_key: Optional[str] = None,
    project_id: Optional[str] = None,
    base_url: str = YADORE_BASE_URL,
    timeout: int = 120,
) -> Dict[str, Any]:
    """GET ``/v2/conversion/general`` — grouped sales/click totals (sanity check)."""
    token = (api_key or YADORE_API_KEY or "").strip()
    if not token:
        raise YadoreClientError("YADORE_API_KEY is not set")
    endpoint = f"{base_url.rstrip('/')}/v2/conversion/general"
    headers = {"Accept": "application/json", "API-Key": token}
    params: Dict[str, Any] = {
        "from": (date_from or "").strip()[:10],
        "to": (date_to or "").strip()[:10],
        "format": "json",
    }
    pid = (project_id or YADORE_PROJECT_ID or "").strip()
    if pid:
        params["projectId"] = pid
    try:
        r = requests.get(endpoint, headers=headers, params=params, timeout=timeout)
    except requests.RequestException as e:
        raise YadoreClientError(str(e)) from e
    if r.status_code != 200:
        raise YadoreClientError(
            f"conversion/general HTTP {r.status_code}",
            status_code=r.status_code,
            response_body=(r.text[:800] if r.text else None),
        )
    try:
        data = r.json() if r.text else {}
    except Exception as e:
        raise YadoreClientError(f"conversion/general JSON error: {e}", response_body=r.text[:500]) from e
    return data if isinstance(data, dict) else {}


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

