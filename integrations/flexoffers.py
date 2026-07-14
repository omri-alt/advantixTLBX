"""
FlexOffers advertiser catalog lookup (no public API — static export JSON).

Catalog: ``data/flexoffers_advertisers.json`` built via
``scripts/build_flexoffers_catalog.py``.
"""
from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CATALOG_PATH = _ROOT / "data" / "flexoffers_advertisers.json"

_STATUS_RANK = {"approved": 3, "pending": 2, "available": 1}


def _norm_geo(geo: str) -> str:
    g = (geo or "").strip().lower()
    if g == "gb":
        return "uk"
    return g[:2] if len(g) >= 2 else g


def _host_key(url_or_host: str) -> str:
    s = (url_or_host or "").strip()
    if not s:
        return ""
    if "://" not in s and "/" not in s and " " not in s:
        h = s.lower().strip(".")
    else:
        u = s if "://" in s else f"https://{s}"
        try:
            h = (urlparse(u).hostname or "").lower().strip(".")
        except Exception:
            return ""
    if h.startswith("www."):
        h = h[4:]
    return h


def _host_candidates(host: str) -> List[str]:
    """Exact + parent hosts (shop.example.co.uk → example.co.uk)."""
    h = _host_key(host)
    if not h:
        return []
    parts = h.split(".")
    out = [h]
    # Drop left-most labels until two remain (or three for multi-part TLD-ish).
    for i in range(1, max(0, len(parts) - 1)):
        cand = ".".join(parts[i:])
        if cand.count(".") >= 1 and cand not in out:
            out.append(cand)
    return out


@lru_cache(maxsize=1)
def _load_catalog(path_str: str) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    path = Path(path_str)
    if not path.exists():
        logger.warning("FlexOffers catalog missing: %s", path)
        return {}, {"count": 0, "path": str(path), "error": "missing"}
    data = json.loads(path.read_text(encoding="utf-8"))
    by_host: Dict[str, List[Dict[str, Any]]] = {}
    for row in data.get("advertisers") or []:
        host = _host_key(str(row.get("host") or row.get("url") or ""))
        if not host:
            continue
        by_host.setdefault(host, []).append(dict(row))
    meta = {
        "count": int(data.get("count") or sum(len(v) for v in by_host.values())),
        "updated_at": data.get("updated_at") or "",
        "source": data.get("source") or "",
        "path": str(path),
    }
    return by_host, meta


def clear_flexoffers_cache() -> None:
    _load_catalog.cache_clear()


def catalog_meta(catalog_path: Optional[Path] = None) -> Dict[str, Any]:
    path = catalog_path or DEFAULT_CATALOG_PATH
    _idx, meta = _load_catalog(str(path))
    return meta


def merchant_monetization_check(
    merchant_url: str,
    country_iso2: str = "",
    *,
    catalog_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Catalog match by **URL hostname only** (FlexOffers geo export is often incomplete).

    ``found`` is True when any catalog advertiser shares the probe host.
    Optional ``country_iso2`` only prefers which row to report when several exist.
    """
    path = catalog_path or DEFAULT_CATALOG_PATH
    by_host, meta = _load_catalog(str(path))
    if not by_host:
        return {
            "found": False,
            "mode": "catalog",
            "note": meta.get("error") or "empty catalog",
            "matched_host": "",
            "advertiser_id": "",
            "name": "",
            "status": "",
            "deeplink": False,
            "geo": "",
        }

    probe_host = _host_key(merchant_url)
    geo = _norm_geo(country_iso2)
    candidates: List[Dict[str, Any]] = []
    matched_host = ""
    for cand in _host_candidates(probe_host):
        rows = by_host.get(cand) or []
        if rows:
            matched_host = cand
            candidates = rows
            break

    if not candidates:
        return {
            "found": False,
            "mode": "catalog",
            "note": "not in flexoffers catalog",
            "matched_host": probe_host,
            "advertiser_id": "",
            "name": "",
            "status": "",
            "deeplink": False,
            "geo": geo,
        }

    # Host match is enough. Prefer same-geo / global rows for display when present.
    def _rank(r: Dict[str, Any]) -> Tuple[int, int, int, int]:
        row_geo = str(r.get("geo") or "")
        geo_pref = 2 if geo and row_geo == geo else (1 if not row_geo else 0)
        return (
            geo_pref,
            _STATUS_RANK.get(str(r.get("status") or ""), 0),
            1 if r.get("deeplink") else 0,
            int(r.get("id") or 0),
        )

    candidates.sort(key=_rank, reverse=True)
    best = candidates[0]
    host_geos = sorted({str(r.get("geo") or "*") for r in candidates})
    return {
        "found": True,
        "mode": "catalog",
        "note": str(best.get("status") or "available"),
        "matched_host": matched_host,
        "advertiser_id": best.get("id") or "",
        "name": str(best.get("name") or ""),
        "status": str(best.get("status") or ""),
        "deeplink": bool(best.get("deeplink")),
        "geo": str(best.get("geo") or ""),
        "url": str(best.get("url") or ""),
        "host_geos": host_geos,
        "catalog_updated_at": meta.get("updated_at") or "",
    }
