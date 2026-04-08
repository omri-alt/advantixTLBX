"""
Kelkoo Publisher Shopping API — merchant URL / deeplink probe (monetization check).

Used by monetization_check.py and monthly merchant log enrichment.
"""
from __future__ import annotations

from typing import Any, Dict

import requests

KELKOO_SEARCH_LINK_URL = "https://api.kelkoogroup.net/publisher/shopping/v2/search/link"


def kelkoo_merchant_link_check(url: str, geo: str, api_key: str) -> Dict[str, Any]:
    """
    GET /publisher/shopping/v2/search/link?country={geo}&merchantUrl={url}

    ``geo`` must be a 2-letter Kelkoo country code (lowercase), e.g. ``de``, ``uk``.
    """
    if not api_key:
        return {"status": "error", "http": "", "found": False, "estimatedCpc": "", "deeplink": "", "raw": "API key missing"}
    headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}
    r = requests.get(
        KELKOO_SEARCH_LINK_URL,
        params={"country": geo, "merchantUrl": url},
        headers=headers,
        timeout=30,
    )
    if r.status_code == 200:
        data = r.json() if r.text else {}
        found = True
        est = data.get("estimatedCpc") or data.get("cpc") or ""
        dl = data.get("url") or data.get("deeplink") or data.get("link") or ""
        return {"status": "ok", "http": 200, "found": found, "estimatedCpc": est, "deeplink": dl, "raw": data}
    if r.status_code == 404:
        try:
            data = r.json()
        except Exception:
            data = r.text
        return {"status": "not_found", "http": 404, "found": False, "estimatedCpc": "", "deeplink": "", "raw": data}
    try:
        data = r.json()
    except Exception:
        data = r.text
    return {"status": "error", "http": r.status_code, "found": False, "estimatedCpc": "", "deeplink": "", "raw": data}


def format_kelkoo_monetization_status(result: Dict[str, Any]) -> str:
    """Human-readable status for spreadsheet column (feed-specific check)."""
    if result.get("status") == "ok" and result.get("found"):
        est = result.get("estimatedCpc")
        if est not in (None, ""):
            return f"monetized (cpc {est})"
        return "monetized"
    if result.get("status") == "not_found":
        return "not_monetized"
    http = result.get("http", "")
    return f"error ({http})" if http != "" else "error"
