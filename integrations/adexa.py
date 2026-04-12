"""
Adexa API helpers (feed4) — Link Monetizer + merchant list.

**Link Monetizer** — GET https://api.adexad.com/LinksMerchant.php
  ?siteID=…&country=…&merchantUrl=…&format=json  (optional merchant_id)

**GetMerchant** — list merchants for a country (uses apiKey + siteID).
  Your working URL shape::

    https://api.adexad.com/v1/GetMerchant/siteID={id}&apiKey={key}&country={geo}&format=json

  ``country`` is passed lowercase (``fr``, ``uk``, …) to match existing scripts.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests

from config import ADEXA_API_KEY, ADEXA_SITE_ID

ADEXA_LINKS_MERCHANT_URL = "https://api.adexad.com/LinksMerchant.php"
ADEXA_GET_MERCHANT_BASE = "https://api.adexad.com/v1/GetMerchant"

# Default geos for bulk scripts (same order as your Adexa feed tooling).
ADEXA_DEFAULT_GEOS: List[str] = [
    "fr",
    "uk",
    "de",
    "it",
    "es",
    "nl",
    "dk",
    "no",
    "se",
    "br",
    "be",
    "at",
    "us",
    "pl",
    "pt",
    "ch",
    "ro",
    "cz",
    "hu",
    "gr",
    "sk",
    "ie",
]


class AdexaClientError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None, response_body: Optional[str] = None):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(message)


def _parse_foundish(payload: Any) -> tuple[bool, str]:
    """
    Best-effort parse: Adexa JSON shapes may vary; look for common keys.
    Returns (found, short_note).
    """
    if payload is None:
        return False, "empty"
    if isinstance(payload, bool):
        return payload, "bool"
    if isinstance(payload, (int, float)):
        return bool(payload), "number"
    if isinstance(payload, str):
        s = payload.strip().lower()
        if s in ("true", "1", "yes", "ok", "success"):
            return True, "string"
        if s in ("false", "0", "no", ""):
            return False, "string"
        return False, "string"

    if isinstance(payload, dict):
        for key in (
            "found",
            "monetized",
            "isMonetized",
            "success",
            "ok",
            "available",
            "hasOffer",
        ):
            if key in payload:
                v = payload[key]
                if isinstance(v, bool):
                    return v, key
                if isinstance(v, (int, float)):
                    return bool(v), key
                if isinstance(v, str) and v.strip().lower() in ("true", "1", "yes", "ok"):
                    return True, key
        # nested data / result
        for nested in ("data", "result", "response", "merchant"):
            if nested in payload and isinstance(payload[nested], dict):
                f, note = _parse_foundish(payload[nested])
                if f or note != "empty":
                    return f, f"{nested}:{note}"
        # array of merchants/offers
        for key in ("merchants", "offers", "items", "links"):
            if key in payload and isinstance(payload[key], list) and len(payload[key]) > 0:
                return True, key
        return False, "no_signal"

    if isinstance(payload, list):
        return len(payload) > 0, "list_len"

    return False, "unknown"


def links_merchant_check(
    merchant_url: str,
    country_iso2: str,
    *,
    site_id: Optional[str] = None,
    merchant_id: Optional[str] = None,
    timeout: int = 45,
) -> Dict[str, Any]:
    """
    Call Adexa Link Monetizer for a merchant URL.

    Returns keys: found (bool), http (int), note (str), raw (parsed JSON or text).
    """
    sid = (site_id or ADEXA_SITE_ID or "").strip()
    if not sid:
        raise AdexaClientError("ADEXA_SITE_ID is not set")

    country = (country_iso2 or "").strip().upper()
    if len(country) != 2:
        raise AdexaClientError(f"Invalid Adexa country: {country_iso2!r}")

    url = (merchant_url or "").strip()
    if not url:
        raise AdexaClientError("merchant_url is empty")

    params: Dict[str, Any] = {
        "siteID": sid,
        "country": country,
        "merchantUrl": url,
        "format": "json",
    }
    if merchant_id:
        params["merchant_id"] = str(merchant_id).strip()

    try:
        # API signals success with HTTP 3xx + Location (tracking URL). Following redirects
        # returns huge HTML and hides the real status — do not follow.
        r = requests.get(
            ADEXA_LINKS_MERCHANT_URL,
            params=params,
            timeout=timeout,
            headers={"Accept": "application/json"},
            allow_redirects=False,
        )
    except requests.RequestException as e:
        raise AdexaClientError(str(e)) from e

    http = r.status_code
    text = r.text or ""
    redirect_url = (r.headers.get("Location") or "").strip()

    # Monetized merchants: 302/301/etc. with a tracking Location (Kelkoo go, Adexa go, …).
    if http in (301, 302, 303, 307, 308):
        if redirect_url:
            return {
                "http": http,
                "found": True,
                "note": "http_redirect",
                "raw": None,
                "country": country,
                "redirect_url": redirect_url[:2000],
            }
        return {
            "http": http,
            "found": False,
            "note": "redirect_no_location",
            "raw": None,
            "country": country,
            "redirect_url": None,
        }

    data: Any = None
    if http == 200 and text:
        try:
            data = r.json()
        except Exception:
            data = {"_raw_text": text[:2000]}

    found, note = _parse_foundish(data)

    # Empty 200 body = not monetized (observed for unknown merchants).
    if http == 200 and not text.strip():
        found = False
        note = "empty_body"

    # If JSON parse failed but HTTP 200, try text heuristics
    if http == 200 and data is None and text:
        tl = text.strip().lower()
        if "true" in tl or "monetiz" in tl or "found" in tl:
            found = found or ("false" not in tl[:200])

    return {
        "http": http,
        "found": bool(found),
        "note": note,
        "raw": data,
        "country": country,
        "redirect_url": redirect_url or None,
    }


def _get_merchant_request_url(site_id: str, api_key: str, country_lower: str) -> str:
    """Build GetMerchant URL the same way as your working scripts (path + &params)."""
    sid = quote(str(site_id).strip(), safe="")
    key = quote(str(api_key).strip(), safe="")
    cty = quote((country_lower or "").strip().lower(), safe="")
    return f"{ADEXA_GET_MERCHANT_BASE}/siteID={sid}&apiKey={key}&country={cty}&format=json"


def get_merchants(
    country: str,
    *,
    site_id: Optional[str] = None,
    api_key: Optional[str] = None,
    timeout: int = 120,
) -> List[Dict[str, Any]]:
    """
    GET GetMerchant for one country. Returns a list of merchant dicts (or empty on bad response).

    Requires ``ADEXA_SITE_ID`` / ``AdexSiteID`` and ``ADEXA_API_KEY`` / ``KeyAdex``.
    Country is normalized to two letters lowercase (``uk``, ``fr``, …).
    """
    sid = (site_id or ADEXA_SITE_ID or "").strip()
    key = (api_key or ADEXA_API_KEY or "").strip()
    if not sid:
        raise AdexaClientError("ADEXA_SITE_ID / AdexSiteID is not set")
    if not key:
        raise AdexaClientError("ADEXA_API_KEY / KeyAdex is not set")

    c = (country or "").strip().lower()[:2]
    if len(c) != 2:
        raise AdexaClientError(f"Invalid country: {country!r}")

    url = _get_merchant_request_url(sid, key, c)
    try:
        r = requests.get(url, timeout=timeout, headers={"Accept": "application/json"})
    except requests.RequestException as e:
        raise AdexaClientError(str(e)) from e

    if r.status_code != 200:
        raise AdexaClientError(
            f"GetMerchant HTTP {r.status_code}",
            status_code=r.status_code,
            response_body=(r.text[:800] if r.text else None),
        )

    try:
        data = r.json()
    except Exception as e:
        raise AdexaClientError(f"GetMerchant JSON parse error: {e}", response_body=r.text[:500]) from e

    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "error" in data:
        raise AdexaClientError(str(data.get("error")), response_body=str(data)[:500])
    if isinstance(data, dict):
        inner = data.get("merchants") or data.get("data") or data.get("result")
        if isinstance(inner, list):
            return inner
    raise AdexaClientError("Unexpected GetMerchant response shape", response_body=str(data)[:500])


ADEXA_STATS_RAW_BASE = "https://api.adexad.com/v1/StatsRaw"


def fetch_stats_raw(
    start: str,
    end: str,
    *,
    site_id: Optional[str] = None,
    api_key: Optional[str] = None,
    nb_page: int = 100,
    timeout: int = 120,
) -> List[Dict[str, Any]]:
    """
    GET ``StatsRaw`` for ``start`` / ``end`` (YYYY-MM-DD), all pages.

    Response JSON is expected to include ``stats`` (list) and ``page`` (last page index, 0-based).
    """
    sid = (site_id or ADEXA_SITE_ID or "").strip()
    key = (api_key or ADEXA_API_KEY or "").strip()
    if not sid:
        raise AdexaClientError("ADEXA_SITE_ID / AdexSiteID is not set")
    if not key:
        raise AdexaClientError("ADEXA_API_KEY / KeyAdex is not set")

    stats: List[Dict[str, Any]] = []

    def fetch_page(page_idx: int) -> tuple[int, List[Dict[str, Any]]]:
        endpoint = (
            f"{ADEXA_STATS_RAW_BASE}/siteID={sid}&apiKey={key}&start={start}&end={end}"
            f"&nb_page={nb_page}&page={page_idx}&format=json"
        )
        r = requests.get(endpoint, timeout=timeout, headers={"Accept": "application/json"})
        if r.status_code != 200:
            raise AdexaClientError(
                f"StatsRaw HTTP {r.status_code}",
                status_code=r.status_code,
                response_body=(r.text[:800] if r.text else None),
            )
        try:
            payload = r.json()
        except Exception as e:
            raise AdexaClientError(f"StatsRaw JSON error: {e}", response_body=r.text[:500]) from e
        if not isinstance(payload, dict):
            raise AdexaClientError("StatsRaw: expected JSON object", response_body=str(payload)[:300])
        rows = payload.get("stats")
        if not isinstance(rows, list):
            rows = []
        last_page = int(payload.get("page", 0) or 0)
        return last_page, rows

    last_page, first_rows = fetch_page(0)
    stats.extend(first_rows)
    for page_idx in range(1, last_page + 1):
        _, page_rows = fetch_page(page_idx)
        stats.extend(page_rows)

    return stats


def filter_static_cpc_with_links(merchants: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """``cpcmodel == 'Static cpc'`` and ``supportsLinks == 1`` (fixim links)."""
    out: List[Dict[str, Any]] = []
    for m in merchants:
        if m.get("cpcmodel") == "Static cpc" and m.get("supportsLinks") == 1:
            out.append(m)
    return out


def filter_static_cpc_offers_only(merchants: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """``cpcmodel == 'Static cpc'`` and ``supportsLinks == 0`` (offers only)."""
    out: List[Dict[str, Any]] = []
    for m in merchants:
        if m.get("cpcmodel") == "Static cpc" and m.get("supportsLinks") == 0:
            out.append(m)
    return out
