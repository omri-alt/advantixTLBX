"""
Daily Adexa Nipuhim merchant pick: **1 merchant per geo per day**.

Both monetization modes are supported; exactly one is chosen per geo:
1. Offers-only Goffers smartlink (preferred when available)
2. Static CPC homepage links / LinksMerchant (fallback)

Sheet Store Link is the golink or homepage; NIPUHIM-adexa sync builds the
matching Keitaro payload. Domain demand: 300 clicks × 1 merchant × geo.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from config import ADEXA_API_KEY, ADEXA_SITE_ID
from geos import is_supported_geo, normalize_geo
from integrations.adexa import (
    ADEXA_DEFAULT_GEOS,
    build_adexa_golink_url,
    extract_adexa_smartlink_url,
    filter_static_cpc_offers_only,
    filter_static_cpc_with_links,
    get_merchants,
    is_adexa_golink_url,
    normalize_merchant_homepage_url,
)

logger = logging.getLogger(__name__)

ADEXA_NIPUHIM_MAX_OFFERS_PER_GEO = 1


def adexa_nipuhim_enabled() -> bool:
    from config import ADEXA_NIPUHIM_ENABLED

    if not ADEXA_NIPUHIM_ENABLED:
        return False
    return bool((ADEXA_SITE_ID or "").strip() and (ADEXA_API_KEY or "").strip())


def adexa_nipuhim_geos() -> List[str]:
    from config import ADEXA_NIPUHIM_GEOS

    out: List[str] = []
    for g in ADEXA_NIPUHIM_GEOS or ADEXA_DEFAULT_GEOS:
        geo = normalize_geo(str(g))
        if len(geo) == 2 and is_supported_geo(geo) and geo not in out:
            out.append(geo)
    return out


def _fnum(x: Any) -> Optional[float]:
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None


def _merchant_cpc(m: Dict[str, Any]) -> float:
    links = m.get("links") if isinstance(m.get("links"), dict) else {}
    offer = m.get("offers") if isinstance(m.get("offers"), dict) else {}
    if not offer and isinstance(m.get("offer"), dict):
        offer = m.get("offer") or {}
    for block in (links, offer):
        if not isinstance(block, dict):
            continue
        for key in ("merchantEstimatedCpc", "staticCpc", "boostCpc", "estimatedCpc"):
            v = _fnum(block.get(key))
            if v is not None and v > 0:
                return v
    return 0.0


def _truthy_flag(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return int(v) != 0
    return str(v or "").strip().lower() in ("1", "true", "yes", "y")


def _resolve_golink(geo: str, m: Dict[str, Any]) -> str:
    mid = str(m.get("id") or m.get("merchantId") or "").strip()
    golink = extract_adexa_smartlink_url(m) or ""
    if golink and is_adexa_golink_url(golink):
        return golink
    if mid:
        return build_adexa_golink_url(geo, mid) or ""
    return ""


def _offers_only_pool(merchants: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for m in merchants:
        if not isinstance(m, dict):
            continue
        if _truthy_flag(m.get("supportsLinks")):
            continue
        if not _truthy_flag(m.get("supportsOffer")):
            continue
        out.append(m)
    return out


def _rank_key(row: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        -int(bool(row.get("randomOffer"))),
        -float(row.get("cpc") or 0),
        str(row.get("name") or ""),
    )


def _smartlink_row(geo: str, m: Dict[str, Any], *, mode: str) -> Optional[Dict[str, Any]]:
    mid = str(m.get("id") or m.get("merchantId") or "").strip()
    name = str(m.get("name") or m.get("merchantName") or "").strip()
    homepage = normalize_merchant_homepage_url(str(m.get("url") or m.get("merchantUrl") or "")) or str(
        m.get("url") or m.get("merchantUrl") or ""
    ).strip()
    golink = _resolve_golink(geo, m)
    if not mid or not golink:
        return None
    return {
        "geo": geo,
        "merchant_id": mid,
        "name": name,
        "url": homepage,
        "golink": golink,
        "store_link": golink,
        "kind": "smartlink",
        "cpc": _merchant_cpc(m),
        "mode": mode,
        "supportsLinks": m.get("supportsLinks"),
        "supportsOffer": m.get("supportsOffer"),
        "randomOffer": bool(extract_adexa_smartlink_url(m)),
        "cpcmodel": str(m.get("cpcmodel") or ""),
    }


def _links_row(geo: str, m: Dict[str, Any], *, mode: str) -> Optional[Dict[str, Any]]:
    mid = str(m.get("id") or m.get("merchantId") or "").strip()
    name = str(m.get("name") or m.get("merchantName") or "").strip()
    homepage = normalize_merchant_homepage_url(str(m.get("url") or m.get("merchantUrl") or "")) or str(
        m.get("url") or m.get("merchantUrl") or ""
    ).strip()
    if not mid or not homepage:
        return None
    return {
        "geo": geo,
        "merchant_id": mid,
        "name": name,
        "url": homepage,
        "golink": "",
        "store_link": homepage,
        "kind": "links",
        "cpc": _merchant_cpc(m),
        "mode": mode,
        "supportsLinks": m.get("supportsLinks"),
        "supportsOffer": m.get("supportsOffer"),
        "randomOffer": bool(extract_adexa_smartlink_url(m)),
        "cpcmodel": str(m.get("cpcmodel") or ""),
    }


def _best_smartlink(
    geo: str,
    merchants: List[Dict[str, Any]],
    *,
    exclude_ids: Optional[set[str]] = None,
) -> Optional[Dict[str, Any]]:
    exclude = exclude_ids or set()
    static_offers = filter_static_cpc_offers_only(merchants)
    pool = static_offers
    mode = "static_offers_smartlink"
    if not pool:
        pool = _offers_only_pool(merchants)
        mode = "offers_only_smartlink"
    ranked: List[Dict[str, Any]] = []
    for m in pool:
        row = _smartlink_row(geo, m, mode=mode)
        if row and str(row["merchant_id"]) not in exclude:
            ranked.append(row)
    if not ranked:
        return None
    ranked.sort(key=_rank_key)
    return ranked[0]


def _best_links(
    geo: str,
    merchants: List[Dict[str, Any]],
    *,
    exclude_ids: Optional[set[str]] = None,
) -> Optional[Dict[str, Any]]:
    exclude = exclude_ids or set()
    pool = filter_static_cpc_with_links(merchants)
    ranked: List[Dict[str, Any]] = []
    for m in pool:
        row = _links_row(geo, m, mode="static_links")
        if row and str(row["merchant_id"]) not in exclude:
            ranked.append(row)
    if not ranked:
        return None
    ranked.sort(key=_rank_key)
    return ranked[0]


def pick_adexa_merchants_one_per_geo(
    geos: Optional[Sequence[str]] = None,
    *,
    sleep_s: float = 0.2,
    exclude_used: Optional[Set[Tuple[str, str]]] = None,
) -> Tuple[Dict[str, List[str]], List[Dict[str, Any]], List[str]]:
    """
    Return ``(chosen, details, logs)`` with exactly **one** merchant id per geo.

    Prefers offers-only smartlink; falls back to static CPC links.

    ``exclude_used`` is a set of ``(geo_lower, merchant_id)`` already used this month
    (from ``{month}_log_adexa``) — skipped so each merchant gets at most one 300-click
    day per calendar month.
    """
    logs: List[str] = []
    chosen: Dict[str, List[str]] = {}
    details: List[Dict[str, Any]] = []
    target = list(geos) if geos is not None else adexa_nipuhim_geos()
    used = exclude_used or set()
    if used:
        logs.append(
            f"Adexa Nipuhim: excluding {len(used)} geo×merchant already used this month"
        )

    for geo in target:
        g = normalize_geo(str(geo))
        if len(g) != 2 or not is_supported_geo(g):
            logs.append(f"Adexa Nipuhim {geo}: unsupported geo — skip")
            continue
        try:
            merchants = get_merchants(g)
        except Exception as e:
            logs.append(f"Adexa Nipuhim {g}: GetMerchant failed: {e}")
            continue
        if sleep_s > 0:
            time.sleep(sleep_s)

        exclude_ids = {mid for (ug, mid) in used if ug == g}
        if exclude_ids:
            logs.append(
                f"Adexa Nipuhim {g}: skipping {len(exclude_ids)} merchant(s) already used this month"
            )

        best = _best_smartlink(g, merchants, exclude_ids=exclude_ids)
        source = "smartlink"
        if not best:
            best = _best_links(g, merchants, exclude_ids=exclude_ids)
            source = "links"

        if not best:
            logs.append(
                f"Adexa Nipuhim {g}: no eligible merchant "
                f"(smartlink/links exhausted or all used this month)"
            )
            continue

        chosen[g] = [str(best["merchant_id"])]
        details.append(best)
        logs.append(
            f"Adexa Nipuhim {g}: picked {best['name']!r} id={best['merchant_id']} "
            f"cpc={best['cpc']} kind={best['kind']} mode={best['mode']} "
            f"(chose={source}) link={str(best['store_link'])[:60]}..."
        )
    return chosen, details, logs


def offer_rows_from_adexa_picks(details: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One sheet row per geo (golink or homepage)."""
    rows: List[Dict[str, Any]] = []
    for d in details:
        geo = str(d.get("geo") or "").strip().lower()
        mid = str(d.get("merchant_id") or "").strip()
        store = str(d.get("store_link") or d.get("golink") or d.get("url") or "").strip()
        name = str(d.get("name") or "").strip() or mid
        kind = str(d.get("kind") or "smartlink").strip().lower()
        if not geo or not mid or not store:
            continue
        tag = "adexa-smartlink" if kind == "smartlink" else "adexa-links"
        rows.append(
            {
                "Country": geo.upper(),
                "Merchant ID": mid,
                "Product Title": f"{name} [{tag}]",
                "Store Link": store,
                "Audit Status": "",
                "Timestamp": "",
            }
        )
    return rows
