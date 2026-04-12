"""
Ecomnia console: sheet-backed geo WL/BL, sync blacklists, whitelist audit rows, exploration action items.

Sheet tab default ``globaList`` columns: ``geo``, ``blacklist``, ``whitelist`` (same as legacy ``ec_local_copy``).
CPC level-up decisions stay in **trackExploration** (``CpcLvlUp`` v/x); this module only surfaces candidates.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import requests

from integrations.ecomnia_geo_lists import (
    audit_whitelist_traffic,
    campaign_daily_budget_value,
    date_range_last_days,
    fetch_adv_stats_by_date,
    fetch_adv_stats_by_source,
    fetch_campaign_by_id,
    fetch_campaigns,
    geo_bw_map_from_rows,
    normalize_geo_key,
    post_update_advertiser_campaign,
    recommended_geo_blacklists,
    spend_on_date_from_daily_stats,
)

logger = logging.getLogger(__name__)


def sheet_a1_values_to_row_dicts(values: Sequence[Sequence[str]]) -> List[Dict[str, str]]:
    """First row = headers (lowercase); following rows → dicts."""
    if not values:
        return []
    header = [str(c or "").strip().lower() for c in values[0]]
    out: List[Dict[str, str]] = []
    for r in values[1:]:
        row: Dict[str, str] = {}
        for i, key in enumerate(header):
            if not key:
                continue
            row[key] = str(r[i] if i < len(r) else "").strip()
        if any(row.values()):
            out.append(row)
    return out


def geo_map_from_sheet_values(values: Sequence[Sequence[str]]) -> Dict[str, Dict[str, List[str]]]:
    rows = sheet_a1_values_to_row_dicts(values)
    return geo_bw_map_from_rows(rows)


def copy_paste_block_for_geo(geo: str, blacklist: Sequence[str], whitelist: Sequence[str]) -> str:
    """Single copy-friendly block for one geo."""
    bl = "\n".join(blacklist) if blacklist else "(empty)"
    wl = "\n".join(whitelist) if whitelist else "(empty)"
    return f"=== {geo.upper()} — BLACKLIST (one source per line) ===\n{bl}\n\n=== {geo.upper()} — WHITELIST (one source per line) ===\n{wl}\n"


def all_copy_paste_text(geo_map: Mapping[str, Mapping[str, Any]]) -> str:
    parts: List[str] = []
    for geo in sorted(geo_map.keys()):
        g = geo_map[geo]
        bl = g.get("blacklist") if isinstance(g.get("blacklist"), list) else []
        wl = g.get("whitelist") if isinstance(g.get("whitelist"), list) else []
        parts.append(copy_paste_block_for_geo(geo, bl, wl))
    return "\n".join(parts).strip()


def _campaign_suffix_wl(name: str) -> bool:
    parts = (name or "").strip().split("-")
    return bool(parts) and parts[-1].strip().lower() == "wl"


def sync_geo_blacklists(
    advertiser_key: str,
    auth_key: str,
    secret_key: str,
    *,
    dry_run: bool = False,
    skip_wl_campaigns: bool = True,
    min_hits_in_geo: int = 4,
    min_total_global: int = 5,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """
    Push per-geo recommended sources into each campaign's ``blacklistsources`` (legacy synch).
    """
    sess = session or requests.Session()
    campaigns = fetch_campaigns(advertiser_key, auth_key, secret_key, session=sess)
    by_geo_rec, global_candidates = recommended_geo_blacklists(
        campaigns,
        min_hits_in_geo=min_hits_in_geo,
        min_total_hits_across_geos=min_total_global,
    )
    log: List[Dict[str, Any]] = []
    errors: List[str] = []
    for c in campaigns:
        if not isinstance(c, dict):
            continue
        nm = str(c.get("name") or "")
        if skip_wl_campaigns and _campaign_suffix_wl(nm):
            continue
        geo = normalize_geo_key(str(c.get("geo") or ""))
        add_list = by_geo_rec.get(geo) or []
        if not add_list:
            continue
        current = c.get("blacklistsources")
        if not isinstance(current, list):
            current = []
        to_add = [s for s in add_list if s not in current]
        if not to_add:
            continue
        cid = str(c.get("id") or "")
        if dry_run:
            log.append({"campaign_id": cid, "campaign_name": nm, "would_add": to_add})
            continue
        full = fetch_campaign_by_id(cid, advertiser_key, auth_key, secret_key, session=sess)
        if not full:
            errors.append(f"fetch failed {cid}")
            continue
        bl = list(full.get("blacklistsources") or [])
        for s in to_add:
            if s not in bl:
                bl.append(s)
        full["blacklistsources"] = bl
        try:
            post_update_advertiser_campaign(cid, full, advertiser_key, auth_key, secret_key, session=sess)
            log.append({"campaign_id": cid, "campaign_name": nm, "added": to_add})
        except Exception as e:
            errors.append(f"{nm}: {e}")
    return {
        "ok": not errors,
        "dry_run": dry_run,
        "global_candidates_count": len(global_candidates),
        "campaign_updates": len(log),
        "log": log,
        "errors": errors,
    }


def whitelist_check_flat_rows(
    advertiser_key: str,
    auth_key: str,
    secret_key: str,
    geo_map: Mapping[str, Mapping[str, Any]],
    *,
    days: int = 30,
    skip_wl_campaigns: bool = True,
    limit_campaigns: int = 0,
    session: Optional[requests.Session] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    One row per (campaign × whitelist source) with click count in the window.
    Returns (flat_rows, summary_per_campaign).
    """
    sess = session or requests.Session()
    campaigns = fetch_campaigns(advertiser_key, auth_key, secret_key, session=sess)
    utc_today = datetime.now(timezone.utc).date()
    start, end = date_range_last_days(utc_today, max(1, days))
    flat: List[Dict[str, Any]] = []
    summaries: List[Dict[str, Any]] = []
    n = 0
    for c in campaigns:
        if not isinstance(c, dict):
            continue
        nm = str(c.get("name") or "")
        if skip_wl_campaigns and _campaign_suffix_wl(nm):
            continue
        if limit_campaigns and n >= limit_campaigns:
            break
        n += 1
        geo = normalize_geo_key(str(c.get("geo") or ""))
        wl = []
        gentry = geo_map.get(geo) or {}
        if isinstance(gentry, dict):
            wl = gentry.get("whitelist") if isinstance(gentry.get("whitelist"), list) else []
        if not wl:
            summaries.append(
                {
                    "campaign_id": str(c.get("id") or ""),
                    "campaign_name": nm,
                    "geo": geo,
                    "note": "no_geo_whitelist_in_sheet",
                    "whitelist_size": 0,
                }
            )
            continue
        try:
            stats = fetch_adv_stats_by_source(
                str(c.get("id") or ""),
                start,
                end,
                advertiser_key,
                auth_key,
                secret_key,
                session=sess,
            )
        except Exception as e:
            summaries.append(
                {
                    "campaign_id": str(c.get("id") or ""),
                    "campaign_name": nm,
                    "geo": geo,
                    "error": str(e),
                }
            )
            continue
        aud = audit_whitelist_traffic(c, wl, stats)
        aud["stats_window_start"] = start
        aud["stats_window_end"] = end
        summaries.append(aud)
        clicks_map = {str(s.get("source") or ""): int(s.get("clicks") or 0) for s in stats if s.get("source")}
        for w in wl:
            w = str(w).strip()
            if not w:
                continue
            cl = int(clicks_map.get(w, 0))
            flat.append(
                {
                    "campaign_id": str(c.get("id") or ""),
                    "campaign_name": nm,
                    "geo": geo,
                    "source": w,
                    "clicks": cl,
                    "had_traffic": cl > 0,
                    "stats_window_start": start,
                    "stats_window_end": end,
                }
            )
    return flat, summaries


def exploration_action_items(
    advertiser_key: str,
    auth_key: str,
    secret_key: str,
    *,
    yesterday_ymd: str,
    source_lookback_days: int = 7,
    max_unbought_list: int = 40,
    skip_wl_campaigns: bool = True,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """
    Active campaigns that **under-spent vs daily budget yesterday** (UTC date row) and have at
    least one **zero-click** source in ``adv-stats-by-source`` over the lookback (not in blacklist).

    Does not change CPC — user uses **trackExploration** ``CpcLvlUp`` v/x with legacy scripts.

    Returns ``{"items": [...], "errors": [...]}``.
    """
    sess = session or requests.Session()
    campaigns = fetch_campaigns(advertiser_key, auth_key, secret_key, session=sess)
    utc_today = datetime.now(timezone.utc).date()
    end = utc_today.isoformat()
    start = (utc_today - timedelta(days=max(1, source_lookback_days))).isoformat()
    items: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for c in campaigns:
        if not isinstance(c, dict):
            continue
        if str(c.get("status") or "").lower() != "active":
            continue
        nm = str(c.get("name") or "")
        if skip_wl_campaigns and _campaign_suffix_wl(nm):
            continue
        budget = campaign_daily_budget_value(c)
        if budget is None or budget <= 0:
            continue
        cid = str(c.get("id") or "")
        try:
            daily = fetch_adv_stats_by_date(
                cid, yesterday_ymd, yesterday_ymd, advertiser_key, auth_key, secret_key, session=sess
            )
        except Exception as e:
            errors.append(
                {
                    "campaign_id": cid,
                    "campaign_name": nm,
                    "geo": normalize_geo_key(str(c.get("geo") or "")),
                    "error": f"daily_stats:{e}",
                }
            )
            continue
        spend_y = spend_on_date_from_daily_stats(daily, yesterday_ymd)
        if spend_y + 1e-6 >= budget:
            continue
        try:
            by_src = fetch_adv_stats_by_source(
                cid, start, end, advertiser_key, auth_key, secret_key, session=sess
            )
        except Exception as e:
            errors.append(
                {
                    "campaign_id": cid,
                    "campaign_name": nm,
                    "geo": normalize_geo_key(str(c.get("geo") or "")),
                    "yesterday_spend": spend_y,
                    "daily_budget": budget,
                    "error": f"by_source:{e}",
                }
            )
            continue
        bl = set(str(x) for x in (c.get("blacklistsources") or []) if x)
        unbought: List[str] = []
        for row in by_src:
            src = str(row.get("source") or "").strip()
            if not src or src in bl:
                continue
            try:
                cl = int(row.get("clicks") or 0)
            except (TypeError, ValueError):
                cl = 0
            if cl == 0:
                unbought.append(src)
        unbought = sorted(set(unbought))[:max_unbought_list]
        if not unbought:
            continue
        items.append(
            {
                "campaign_id": cid,
                "campaign_name": nm,
                "geo": normalize_geo_key(str(c.get("geo") or "")),
                "yesterday_date": yesterday_ymd,
                "yesterday_spend": round(spend_y, 4),
                "daily_budget": budget,
                "budget_gap": round(max(0.0, budget - spend_y), 4),
                "unbought_zero_click_sources": unbought,
                "unbought_count": len(unbought),
                "source_stats_window": f"{start} .. {end}",
                "sheet_hint": "Set CpcLvlUp to v in trackExploration after review; x skips.",
            }
        )
    return {"items": items, "errors": errors}


def utc_yesterday_iso() -> str:
    d = datetime.now(timezone.utc).date() - timedelta(days=1)
    return d.isoformat()
