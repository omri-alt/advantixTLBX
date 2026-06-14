"""
Adexa API helpers (feed4) — Link Monetizer + merchant list.

**Link Monetizer** — GET https://api.adexad.com/LinksMerchant.php
  ?siteID=…&country=…&merchantUrl=…&format=json  (optional merchant_id).
  ``country`` is sent lowercase (``fr``, ``uk``, …), same as GetMerchant and Kelkoo-style geos.

**Smartlink fallback** — when LinksMerchant returns empty but ``GetMerchant`` has
``supportsOffer: 1`` and ``randomOffer`` (Goffers golink), ``merchant_monetization_check``
reports ``mode=smartlink`` and builds a Keitaro golink URL with ``clickid={subid}``.

When both LinksMerchant and golink work, ``mode=links+smartlink`` and both paths are reported.
``normalize_merchant_homepage_url()`` fixes common ``www`` typos (e.g. ``wwwlampenwelt.de``).

**GetMerchant** — list merchants for a country (uses apiKey + siteID).
  Your working URL shape::

    https://api.adexad.com/v1/GetMerchant/siteID={id}&apiKey={key}&country={geo}&format=json

  ``country`` is passed lowercase (``fr``, ``uk``, …) to match existing scripts.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urlparse

import requests

from config import ADEXA_API_KEY, ADEXA_SITE_ID
from integrations.monetization_geo import two_letter_lower

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


def _looks_like_domain(host: str) -> bool:
    host = (host or "").strip().lower()
    if not host or " " in host or "/" in host:
        return False
    if host.startswith("www."):
        host = host[4:]
    parts = host.split(".")
    if len(parts) < 2:
        return False
    tld = parts[-1]
    return len(tld) >= 2 and tld.isalpha()


def infer_merchant_url_from_adexa_name(name: str) -> str:
    """
    Best-effort homepage host when GetMerchant has no URL but the stats name encodes the domain.

    Examples:
      - ``courir.com/wwwcourircom`` → ``www.courir.com``
      - ``example.co.uk`` → ``www.example.co.uk``
    """
    raw = (name or "").strip()
    if not raw:
        return ""
    if raw.lower().startswith(("http://", "https://")):
        return raw
    if " " in raw and "/" not in raw:
        return ""

    candidate = raw
    if "/" in raw:
        left, right = raw.split("/", 1)
        left = left.strip()
        right = right.strip().lower()
        if _looks_like_domain(left):
            base = left.lower()
            compact = base.replace(".", "")
            if right.startswith("www") and not right.startswith("www."):
                if right == f"www{compact}":
                    return f"www.{base}" if not base.startswith("www.") else base
            candidate = left

    host = candidate.strip().lower()
    if not _looks_like_domain(host):
        return ""
    if not host.startswith("www."):
        return f"www.{host}"
    return host


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


def normalize_merchant_homepage_url(url: str) -> str:
    """
    Fix glued ``www`` host typos: ``https://wwwlampenwelt.de`` → ``https://www.lampenwelt.de``.

    Applied before Adexa probes and feed-balance URL inference so LinksMerchant is not
    skipped due to bootstrap/CSV mistakes.
    """
    raw = (url or "").strip()
    if not raw:
        return ""
    if not raw.lower().startswith(("http://", "https://")):
        raw = f"https://{raw.lstrip('/')}"
    parsed = urlparse(raw)
    host = (parsed.hostname or "").lower()
    if not host:
        return raw
    if host.startswith("www") and not host.startswith("www."):
        rest = host[3:]
        if _looks_like_domain(rest):
            scheme = parsed.scheme or "https"
            path = parsed.path or ""
            query = f"?{parsed.query}" if parsed.query else ""
            return f"{scheme}://www.{rest}{path}{query}"
    return raw


def _merchant_host_key(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    if not raw.lower().startswith(("http://", "https://")):
        raw = f"https://{raw.lstrip('/')}"
    host = (urlparse(raw).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _truthy_flag(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return int(v) != 0
    return str(v or "").strip().lower() in ("1", "true", "yes", "y")


def extract_adexa_smartlink_url(merchant: Dict[str, Any]) -> str:
    """Golink / Goffers URL when homepage links are unavailable but offers are."""
    if not isinstance(merchant, dict):
        return ""
    for key in ("randomOffer", "golink", "goLink", "smartlink", "smartLink"):
        val = str(merchant.get(key) or "").strip()
        if val.lower().startswith("http"):
            return val
    offer = merchant.get("offer")
    if isinstance(offer, dict):
        for key in ("url", "golink", "goLink", "randomOffer"):
            val = str(offer.get(key) or "").strip()
            if val.lower().startswith("http"):
                return val
    return ""


def merchant_supports_adexa_smartlink(merchant: Dict[str, Any]) -> bool:
    if not isinstance(merchant, dict):
        return False
    if not _truthy_flag(merchant.get("supportsOffer")):
        return False
    return bool(extract_adexa_smartlink_url(merchant))


def find_get_merchant_by_url(
    merchant_url: str,
    country_iso2: str,
    *,
    site_id: Optional[str] = None,
    api_key: Optional[str] = None,
    merchants: Optional[List[Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    """Match a GetMerchant row by homepage host (``url`` field)."""
    needle = _merchant_host_key(merchant_url)
    if not needle:
        return None
    rows = merchants
    if rows is None:
        try:
            rows = get_merchants(country_iso2, site_id=site_id, api_key=api_key)
        except AdexaClientError:
            return None
    best: Optional[Tuple[int, Dict[str, Any]]] = None
    for m in rows or []:
        if not isinstance(m, dict):
            continue
        host = _merchant_host_key(str(m.get("url") or m.get("merchantUrl") or ""))
        if not host:
            continue
        score = 0
        if host == needle:
            score = 100
        elif needle.endswith("." + host) or host.endswith("." + needle):
            score = 80
        elif needle in host or host in needle:
            score = 50
        if score and (best is None or score > best[0]):
            best = (score, m)
    return best[1] if best else None


def build_adexa_golink_keitaro_payload(
    golink_url: str,
    *,
    clickid_macro: str = "{subid}",
) -> str:
    """
    Keitaro offer URL for an Adexa smartlink (Goffers / golink).

    Appends ``clickid={subid}`` when missing — same macro used on feed4 dynamic offers.
    """
    base = (golink_url or "").strip()
    if not base:
        return ""
    if "clickid=" not in base.lower():
        # Goffers URLs often look like ``.../Goffers/country=FR&mid=...`` (no ``?``).
        sep = "&" if ("?" in base or "=" in base.split("//", 1)[-1]) else "?"
        base = f"{base}{sep}clickid={clickid_macro}"
    return base


def _apply_get_merchant_fields(out: Dict[str, Any], merchant: Dict[str, Any]) -> None:
    out["merchant_id"] = str(merchant.get("id") or merchant.get("merchantId") or "")
    out["merchant_name"] = str(merchant.get("name") or "")
    out["cpc_model"] = str(merchant.get("cpcmodel") or merchant.get("cpcModel") or "")
    out["raw_merchant"] = merchant
    offer_block = merchant.get("offer")
    if isinstance(offer_block, dict):
        cpc = offer_block.get("boostCpc") or offer_block.get("staticCpc")
        if cpc is not None:
            out["estimated_cpc"] = str(cpc)
    links_block = merchant.get("links")
    if not out.get("estimated_cpc") and isinstance(links_block, dict):
        cpc = links_block.get("merchantEstimatedCpc")
        if cpc is not None:
            out["estimated_cpc"] = str(cpc)


def merchant_monetization_check(
    merchant_url: str,
    country_iso2: str,
    *,
    site_id: Optional[str] = None,
    api_key: Optional[str] = None,
    merchants: Optional[List[Dict[str, Any]]] = None,
    timeout: int = 45,
) -> Dict[str, Any]:
    """
    Adexa monetization for checkmon / feed balance.

    1. ``LinksMerchant.php`` homepage probe (redirect = monetized links).
    2. ``GetMerchant`` golink when ``supportsOffer`` + ``randomOffer``.

    Returns ``found`` True for either path. ``mode`` is ``links``, ``smartlink``,
    ``links+smartlink``, or ``none``. ``keitaro_offer_url`` is set when golink is available.
    """
    probe_url = normalize_merchant_homepage_url(merchant_url) or (merchant_url or "").strip()
    links = links_merchant_check(
        probe_url,
        country_iso2,
        site_id=site_id,
        merchant_id=None,
        timeout=timeout,
    )
    links_found = bool(links.get("found"))
    out: Dict[str, Any] = {
        "found": links_found,
        "mode": "links" if links_found else "none",
        "note": str(links.get("note") or ""),
        "http": links.get("http"),
        "country": links.get("country"),
        "redirect_url": links.get("redirect_url"),
        "links_found": links_found,
        "smartlink_found": False,
        "smartlink_url": "",
        "keitaro_offer_url": "",
        "merchant_id": "",
        "merchant_name": "",
        "cpc_model": "",
        "estimated_cpc": "",
        "operator_hint": "",
        "raw_merchant": None,
        "probe_url": probe_url,
    }

    merchant = find_get_merchant_by_url(
        probe_url,
        country_iso2,
        site_id=site_id,
        api_key=api_key,
        merchants=merchants,
    )
    smartlink_found = False
    golink = ""
    if merchant:
        _apply_get_merchant_fields(out, merchant)
        golink = extract_adexa_smartlink_url(merchant) or ""
        smartlink_found = bool(golink and merchant_supports_adexa_smartlink(merchant))
        if smartlink_found:
            out["smartlink_found"] = True
            out["smartlink_url"] = golink
            out["keitaro_offer_url"] = build_adexa_golink_keitaro_payload(golink)

    if links_found and smartlink_found:
        out.update(
            {
                "found": True,
                "mode": "links+smartlink",
                "note": "http_redirect+smartlink_goffers",
                "operator_hint": (
                    "Homepage monetized via LinksMerchant (merchantUrl + clickid). "
                    "Golink smartlink also available as alternate offer."
                ),
            }
        )
        return out

    if links_found:
        out["operator_hint"] = "Use dynamic LinksMerchant offer (merchantUrl + clickid)."
        return out

    if smartlink_found:
        out.update(
            {
                "found": True,
                "mode": "smartlink",
                "note": "smartlink_goffers",
                "operator_hint": (
                    "Homepage not monetized via LinksMerchant; use golink smartlink offer "
                    f"for {out['merchant_name'] or 'merchant'} instead of dynamic merchantUrl offer."
                ),
            }
        )
        return out

    if merchant:
        if _truthy_flag(merchant.get("supportsLinks")):
            out["note"] = out["note"] or "supports_links_but_probe_failed"
        elif _truthy_flag(merchant.get("supportsOffer")):
            out["note"] = out["note"] or "supports_offer_missing_golink"
        else:
            out["note"] = out["note"] or "merchant_no_links_or_offer"
    else:
        out["note"] = out["note"] or "merchant_not_in_getmerchant"
    return out


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

    g = two_letter_lower(country_iso2 or "")
    if len(g) != 2:
        raise AdexaClientError(f"Invalid Adexa country: {country_iso2!r}")
    if g == "gb":
        g = "uk"
    country = g

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
ADEXA_SHOPPING_SEARCH_STATS_BASE = "https://api.adexad.com/v1/GetShoppingSearchStats"


def _adexa_get_with_retry(url: str, *, timeout: int, max_attempts: int = 8) -> requests.Response:
    """
    GET with backoff on rate limit / transient overload (429 / 503).
    """
    wait_s = 2.0
    last: Optional[requests.Response] = None
    for attempt in range(max(1, max_attempts)):
        r = requests.get(url, timeout=timeout, headers={"Accept": "application/json"})
        last = r
        if r.status_code in (429, 503) and attempt < max_attempts - 1:
            time.sleep(wait_s)
            wait_s = min(wait_s * 2.0, 45.0)
            continue
        return r
    assert last is not None
    return last


def fetch_shopping_search_stats(
    start: str,
    end: str,
    *,
    site_id: Optional[str] = None,
    api_key: Optional[str] = None,
    nb_page: int = 100,
    timeout: int = 120,
) -> List[Dict[str, Any]]:
    """
    GET ``GetShoppingSearchStats`` for ``start`` / ``end`` (YYYY-MM-DD), all pages.

    Paged like ``StatsRaw``: ``stats`` list + ``page`` (last page index, 0-based).
    Row shapes vary; callers should use tolerant field accessors.
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
            f"{ADEXA_SHOPPING_SEARCH_STATS_BASE}/siteID={sid}&apiKey={key}&start={start}&end={end}"
            f"&nb_page={nb_page}&page={page_idx}&format=json"
        )
        r = _adexa_get_with_retry(endpoint, timeout=timeout)
        if r.status_code != 200:
            raise AdexaClientError(
                f"GetShoppingSearchStats HTTP {r.status_code}",
                status_code=r.status_code,
                response_body=(r.text[:800] if r.text else None),
            )
        try:
            payload = r.json()
        except Exception as e:
            raise AdexaClientError(f"GetShoppingSearchStats JSON error: {e}", response_body=r.text[:500]) from e
        if not isinstance(payload, dict):
            raise AdexaClientError(
                "GetShoppingSearchStats: expected JSON object", response_body=str(payload)[:300]
            )
        rows = payload.get("stats")
        if not isinstance(rows, list):
            for alt in ("data", "results", "items", "merchants"):
                inner = payload.get(alt)
                if isinstance(inner, list):
                    rows = inner
                    break
            else:
                rows = []
        last_page = int(payload.get("page", 0) or 0)
        return last_page, [x for x in rows if isinstance(x, dict)]

    last_page, first_rows = fetch_page(0)
    stats.extend(first_rows)
    for page_idx in range(1, last_page + 1):
        _, page_rows = fetch_page(page_idx)
        stats.extend(page_rows)

    return stats


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
        r = _adexa_get_with_retry(endpoint, timeout=timeout)
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
