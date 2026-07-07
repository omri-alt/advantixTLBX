"""
Hub campaign stream weights from Blend clickCap totals and Nipuhim offer-slot counts.

Blend: sum ``clickCap`` per ``feed`` on the Blend sheet (rows with cap > 0).
Nipuhim: count of PLA offer slots per feed from today's ``{date}_offers_*`` tabs
(one slot per geo row up to ``max_offers_per_geo``; no clickCap column on Nipuhim).
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, FrozenSet, List, Optional, Tuple


def _utc_today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _nipuhim_sheet_for_feed(date_str: str, feed_key: str) -> Optional[str]:
    fk = (feed_key or "").strip().lower()
    if fk == "kelkoo1":
        return f"{date_str}_offers_1"
    if fk == "kelkoo2":
        return f"{date_str}_offers_2"
    if fk == "kelkoo5":
        return f"{date_str}_offers_5"
    return None


def blend_feed_click_caps(*, sheets_service: Any = None) -> Tuple[Dict[str, float], List[str]]:
    """Sum Blend sheet ``clickCap`` per feed tag (only rows with cap > 0)."""
    logs: List[str] = []
    caps: Dict[str, float] = defaultdict(float)
    try:
        from blend_sync_from_sheet import get_sheets_service, read_blend_rows
    except Exception as e:
        return {}, [f"Blend clickCap: import failed: {e}"]

    try:
        service = sheets_service or get_sheets_service()
        rows = read_blend_rows(service)
    except Exception as e:
        return {}, [f"Blend clickCap: sheet read failed: {e}"]

    for row in rows:
        cap = float(row.click_cap or 0)
        if cap <= 0:
            continue
        fk = (row.feed_tag or "kelkoo1").strip().lower()
        caps[fk] += cap

    if caps:
        parts = [f"{fk}={int(v) if v == int(v) else v}" for fk, v in sorted(caps.items())]
        logs.append(f"Blend clickCap totals: {', '.join(parts)}")
    else:
        logs.append("Blend clickCap totals: none (no rows with clickCap > 0)")
    return dict(caps), logs


def nipuhim_feed_offer_slots(
    *,
    date_str: Optional[str] = None,
    max_offers_per_geo: int = 60,
) -> Tuple[Dict[str, float], List[str]]:
    """
    Count Nipuhim offer slots per Kelkoo feed from today's offers tabs.

    Each store-link row (per geo, capped at ``max_offers_per_geo``) counts as one slot.
    """
    feed_geos, logs = nipuhim_feed_active_geos(
        date_str=date_str,
        max_offers_per_geo=max_offers_per_geo,
    )
    caps = {fk: float(len(geos)) for fk, geos in feed_geos.items()}
    if not caps:
        logs.append(f"Nipuhim offer slots: no data for {(date_str or _utc_today_str()).strip()} offers tabs")
    return caps, logs


def nipuhim_feed_active_geos(
    *,
    date_str: Optional[str] = None,
    max_offers_per_geo: int = 60,
    feed_keys: Tuple[str, ...] = ("kelkoo1", "kelkoo2", "kelkoo5"),
) -> Tuple[Dict[str, FrozenSet[str]], List[str]]:
    """
    Geos with at least one store-link row on today's ``{date}_offers_*`` tab per feed.
    """
    from geos import normalize_geo
    from update_offers_from_sheet import read_sheet_today_offers

    day = (date_str or _utc_today_str()).strip()
    logs: List[str] = []
    out: Dict[str, frozenset[str]] = {}

    for fk in feed_keys:
        sheet = _nipuhim_sheet_for_feed(day, fk)
        if not sheet:
            continue
        try:
            by_geo, _ = read_sheet_today_offers(sheet, max_per_geo=max_offers_per_geo)
        except Exception as e:
            logs.append(f"Nipuhim {fk}: could not read {sheet!r}: {e}")
            continue
        geos = frozenset(
            normalize_geo(g)
            for g, links in (by_geo or {}).items()
            if links
        )
        out[fk] = geos
        if geos:
            logs.append(
                f"Nipuhim {fk}: {len(geos)} geo(s) with offers from {sheet!r} "
                f"({', '.join(sorted(geos))})"
            )
        else:
            logs.append(f"Nipuhim {fk}: 0 geos with offers from {sheet!r}")

    return out, logs


def hub_nipuhim_equal_weights_per_geo(
    feed_geos: Dict[str, FrozenSet[str]],
    *,
    active_feeds: frozenset[str],
) -> Tuple[Dict[str, Dict[str, float]], List[str]]:
    """
    Per-geo hub weights: equal split across feeds that have offers for that geo.

    Example: if kelkoo1+kelkoo2+kelkoo5 all have ``de`` offers → 33.33% each on ``de_*`` streams.
    If only kelkoo1 has ``au`` offers → 100% to ``hub_nipuhim_kelkoo1`` on ``au_*`` streams.
    """
    all_geos: set[str] = set()
    for fk in active_feeds:
        all_geos |= set(feed_geos.get(fk, frozenset()))

    weights_by_geo: Dict[str, Dict[str, float]] = {}
    logs: List[str] = []
    for geo in sorted(all_geos):
        feeds_with_geo = sorted(fk for fk in active_feeds if geo in feed_geos.get(fk, frozenset()))
        if not feeds_with_geo:
            continue
        share = 100.0 / len(feeds_with_geo)
        weights_by_geo[geo] = {f"hub_nipuhim_{fk}": share for fk in feeds_with_geo}
        logs.append(
            f"Hub {geo}: {len(feeds_with_geo)} feed(s) @ {share:.2f}% each "
            f"({', '.join(feeds_with_geo)})"
        )
    return weights_by_geo, logs


def hub_offer_weights_from_caps(
    blend_feed_caps: Dict[str, float],
    nipuhim_feed_caps: Dict[str, float],
    *,
    active_feeds: frozenset[str],
    hub_types: Tuple[str, ...] = ("blend", "nipuhim"),
) -> Dict[str, float]:
    """Convert per-feed capacity numbers into hub offer weight percentages."""
    use_blend = "blend" in hub_types
    use_nipuhim = "nipuhim" in hub_types
    blend_total = (
        sum(max(0.0, float(blend_feed_caps.get(fk, 0))) for fk in active_feeds)
        if use_blend
        else 0.0
    )
    nipuhim_total = (
        sum(max(0.0, float(nipuhim_feed_caps.get(fk, 0))) for fk in active_feeds)
        if use_nipuhim
        else 0.0
    )
    type_total = blend_total + nipuhim_total
    if type_total <= 0:
        return {}

    out: Dict[str, float] = {}
    for hub_type, feed_caps, type_sum in (
        ("blend", blend_feed_caps, blend_total),
        ("nipuhim", nipuhim_feed_caps, nipuhim_total),
    ):
        if hub_type not in hub_types:
            continue
        type_frac = type_sum / type_total
        feed_total = sum(max(0.0, float(feed_caps.get(fk, 0))) for fk in active_feeds)
        for feed_key in active_feeds:
            name = f"hub_{hub_type}_{feed_key}"
            if feed_total <= 0:
                out[name] = 0.0
                continue
            cap = max(0.0, float(feed_caps.get(feed_key, 0)))
            if cap <= 0:
                out[name] = 0.0
                continue
            out[name] = type_frac * (cap / feed_total) * 100.0
    return out
