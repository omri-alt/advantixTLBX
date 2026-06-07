"""
Shopnomix demand API (feed6) — monetization checks via ``GET /api/v2/demand/:campaign_id``.

Two placements: tile (native / non-coupon) and coupons — each has its own campaign id.
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from config import (
    SHOPNOMIX_BASE_URL,
    SHOPNOMIX_COUPONS_CAMPAIGN_ID,
    SHOPNOMIX_TILE_CAMPAIGN_ID,
)
from integrations.monetization_geo import geo_for_shopnomix

logger = logging.getLogger(__name__)

_CACHE_LOCK = threading.Lock()
_DEMAND_INDEX: Dict[Tuple[str, str], Dict[str, List[Dict[str, Any]]]] = {}


class ShopnomixClientError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None, response_body: Optional[str] = None):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(message)


def clear_demand_cache() -> None:
    with _CACHE_LOCK:
        _DEMAND_INDEX.clear()


def root_domain_from_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if not u.startswith(("http://", "https://")):
        u = "https://" + u.lstrip("/")
    host = (urlparse(u).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _domains_match(url_host: str, api_root: str) -> bool:
    url_host = (url_host or "").lower().strip()
    api_root = (api_root or "").lower().strip()
    if not url_host or not api_root:
        return False
    if url_host == api_root:
        return True
    return url_host.endswith("." + api_root)


def _fetch_demand_page(
    *,
    campaign_id: str,
    geo_code: str,
    base_url: str,
    next_url: Optional[str],
    timeout: int,
) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[str]]:
    if next_url:
        try:
            r = requests.get(next_url, timeout=timeout)
        except requests.RequestException as e:
            return [], None, str(e)
    else:
        url = f"{base_url.rstrip('/')}/api/v2/demand/{campaign_id}"
        try:
            r = requests.get(
                url,
                params={"country_codes": geo_code, "limit": 1000},
                timeout=timeout,
            )
        except requests.RequestException as e:
            return [], None, str(e)

    if r.status_code != 200:
        return [], None, f"HTTP {r.status_code}: {(r.text or '')[:200]}"

    try:
        payload = r.json() if r.text else {}
    except Exception as e:
        return [], None, str(e)

    items = payload.get("data") if isinstance(payload, dict) else None
    rows = [it for it in items if isinstance(it, dict)] if isinstance(items, list) else []
    meta = payload.get("meta") if isinstance(payload, dict) else {}
    nxt = meta.get("next_page") if isinstance(meta, dict) else None
    return rows, (str(nxt).strip() if nxt else None), None


def _load_demand_index(
    campaign_id: str,
    geo_code: str,
    *,
    base_url: Optional[str] = None,
    timeout: int = 45,
    target_host: Optional[str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    cid = (campaign_id or "").strip()
    geo_c = geo_for_shopnomix(geo_code)
    base = (base_url or SHOPNOMIX_BASE_URL or "https://r.v2i8b.com").strip().rstrip("/")
    cache_key = (cid, geo_c)
    host_needle = root_domain_from_url(target_host) if target_host else ""
    with _CACHE_LOCK:
        if cache_key in _DEMAND_INDEX:
            return _DEMAND_INDEX[cache_key]

    index: Dict[str, List[Dict[str, Any]]] = {}
    next_url: Optional[str] = None
    last_err: Optional[str] = None
    matched_early = False
    for _ in range(500):
        rows, next_url, err = _fetch_demand_page(
            campaign_id=cid,
            geo_code=geo_c,
            base_url=base,
            next_url=next_url,
            timeout=timeout,
        )
        if err:
            last_err = err
            break
        for row in rows:
            root = str(row.get("root_domain") or "").lower().strip()
            if not root:
                continue
            index.setdefault(root, []).append(row)
            if host_needle and _domains_match(host_needle, root) and _pick_item(index[root], geo_code):
                matched_early = True
        if matched_early or not next_url:
            break

    if not index and last_err:
        raise ShopnomixClientError(last_err)

    with _CACHE_LOCK:
        _DEMAND_INDEX[cache_key] = index
    return index


def _pick_item(items: List[Dict[str, Any]], geo_code: str) -> Optional[Dict[str, Any]]:
    geo_c = geo_for_shopnomix(geo_code)
    exact = None
    global_row = None
    for row in items:
        cc_raw = row.get("country_code")
        cc = str(cc_raw or "").strip().lower() if cc_raw is not None else ""
        if not cc:
            global_row = global_row or row
        elif cc == geo_c:
            exact = row
            break
    return exact or global_row


def demand_merchant_check(
    merchant_url: str,
    country_iso2: str,
    *,
    campaign_id: Optional[str] = None,
    base_url: Optional[str] = None,
    timeout: int = 45,
    early_exit: bool = True,
) -> Dict[str, Any]:
    """
    Match ``merchant_url`` root domain against Shopnomix demand for ``campaign_id`` + geo.

    ``early_exit=True`` (manual checks) stops paging once a match is found.
    ``early_exit=False`` (sheet bulk) walks the full catalog once per campaign+geo for cache reuse.
    """
    cid = (campaign_id or "").strip()
    if not cid:
        return {"found": False, "note": "Shopnomix campaign id not configured"}

    host = root_domain_from_url(merchant_url)
    if not host:
        return {"found": False, "note": "empty url"}

    try:
        index = _load_demand_index(
            cid,
            country_iso2,
            base_url=base_url,
            timeout=timeout,
            target_host=merchant_url if early_exit else None,
        )
    except ShopnomixClientError as e:
        return {"found": False, "note": str(e)[:200]}

    for api_root, items in index.items():
        if not _domains_match(host, api_root):
            continue
        picked = _pick_item(items, country_iso2)
        if not picked:
            continue
        epc = picked.get("epc")
        return {
            "found": True,
            "epc": epc,
            "deeplink": picked.get("deeplink"),
            "root_domain": picked.get("root_domain") or api_root,
            "matched_host": host,
            "brand_name": picked.get("brand_name") or "",
        }

    return {"found": False, "matched_host": host, "note": "not in demand catalog"}


def demand_tile_check(merchant_url: str, country_iso2: str, **kwargs: Any) -> Dict[str, Any]:
    return demand_merchant_check(
        merchant_url,
        country_iso2,
        campaign_id=kwargs.pop("campaign_id", None) or SHOPNOMIX_TILE_CAMPAIGN_ID,
        **kwargs,
    )


def demand_coupons_check(merchant_url: str, country_iso2: str, **kwargs: Any) -> Dict[str, Any]:
    return demand_merchant_check(
        merchant_url,
        country_iso2,
        campaign_id=kwargs.pop("campaign_id", None) or SHOPNOMIX_COUPONS_CAMPAIGN_ID,
        **kwargs,
    )
