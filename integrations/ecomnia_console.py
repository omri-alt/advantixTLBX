"""
Ecomnia console: sheet geo BL/WL, **campaign** ``whitelistsources`` from API, global WL (≥2 campaigns),
sync blacklists, whitelist audit rows, exploration action items.

Sheet tab default ``globaList`` columns: ``geo``, ``blacklist``, ``whitelist`` (legacy ``ec_local_copy``).
Whitelist checks use **merged** lists: campaign API whitelist ∪ sheet whitelist for that geo.
CPC level-up stays in **trackExploration** (``CpcLvlUp`` v/x); this module only surfaces candidates.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import requests

from integrations.ecomnia_geo_lists import (
    audit_whitelist_traffic,
    campaign_daily_budget_value,
    campaign_default_bid_value,
    campaign_whitelist_sources,
    clicks_for_source_from_stats,
    conversions_field_present_in_stat,
    conversions_from_source_stat,
    cpc_for_source_from_campaign,
    date_range_last_days,
    fetch_adv_stats_by_date,
    fetch_adv_stats_by_source,
    fetch_campaign_by_id,
    fetch_campaigns,
    find_adv_stats_row_for_source,
    geo_bw_map_from_rows,
    global_whitelist_sources,
    merged_contains_source,
    merged_whitelist_for_campaign,
    normalize_geo_key,
    post_update_advertiser_campaign,
    recommended_geo_blacklists,
    spend_on_date_from_daily_stats,
    whitelist_union_by_geo_from_campaigns,
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


def derived_whitelist_copy_paste(derived: Mapping[str, Any]) -> str:
    """Global WL + per-geo union from campaigns (for UI paste)."""
    lines: List[str] = []
    gw = derived.get("global_whitelist") or []
    if isinstance(gw, list) and gw:
        lines.append("=== GLOBAL WHITELIST (source on ≥2 campaigns) ===")
        for item in gw:
            if isinstance(item, dict):
                s = str(item.get("source") or "")
                n = item.get("campaign_count", "")
                lines.append(f"{s}\t({n} campaigns)" if n != "" else s)
            else:
                lines.append(str(item))
        lines.append("")
    byg = derived.get("whitelist_by_geo") or {}
    if isinstance(byg, dict):
        for geo in sorted(byg.keys()):
            wl = byg[geo]
            if not isinstance(wl, list):
                continue
            lines.append(f"=== {str(geo).upper()} — WHITELIST (union from campaigns) ===")
            lines.extend(wl if wl else ["(empty)"])
            lines.append("")
    return "\n".strip()


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


def build_derived_whitelist_views(
    campaigns: Sequence[Mapping[str, Any]],
    *,
    global_min_campaigns: int = 2,
) -> Dict[str, Any]:
    """
    Per-campaign WL from API, global WL (source on ≥2 campaigns), union WL by geo.
    """
    per_c: List[Dict[str, Any]] = []
    for c in campaigns:
        if not isinstance(c, dict):
            continue
        wl = campaign_whitelist_sources(c)
        per_c.append(
            {
                "campaign_id": str(c.get("id") or ""),
                "campaign_name": str(c.get("name") or ""),
                "geo": normalize_geo_key(str(c.get("geo") or "")),
                "sources": wl,
                "whitelist_size": len(wl),
            }
        )
    per_c.sort(key=lambda x: (x.get("geo") or "", x.get("campaign_name") or ""))
    pairs = global_whitelist_sources(campaigns, min_campaigns=global_min_campaigns)
    global_list = [{"source": s, "campaign_count": n} for s, n in pairs]
    by_geo = whitelist_union_by_geo_from_campaigns(campaigns)
    return {
        "global_whitelist": global_list,
        "whitelist_by_geo": by_geo,
        "per_campaign": per_c,
        "global_min_campaigns": global_min_campaigns,
    }


def pull_derived_whitelist_with_campaigns(
    advertiser_key: str,
    auth_key: str,
    secret_key: str,
    *,
    global_min_campaigns: int = 2,
    session: Optional[requests.Session] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Fetch campaigns once; return ``(derived_views, campaigns)`` for WL + potential counts."""
    sess = session or requests.Session()
    campaigns = fetch_campaigns(advertiser_key, auth_key, secret_key, session=sess)
    out = build_derived_whitelist_views(campaigns, global_min_campaigns=global_min_campaigns)
    out["campaigns_fetched"] = len([c for c in campaigns if isinstance(c, dict)])
    return out, campaigns


def pull_derived_whitelist_from_api(
    advertiser_key: str,
    auth_key: str,
    secret_key: str,
    *,
    global_min_campaigns: int = 2,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """Fetch campaigns and build global / per-geo / per-campaign whitelist views."""
    derived, _camps = pull_derived_whitelist_with_campaigns(
        advertiser_key,
        auth_key,
        secret_key,
        global_min_campaigns=global_min_campaigns,
        session=session,
    )
    return derived


def compute_global_wl_zero_click_potential(
    campaigns: Sequence[Mapping[str, Any]],
    geo_map: Mapping[str, Mapping[str, Any]],
    advertiser_key: str,
    auth_key: str,
    secret_key: str,
    *,
    days: int = 30,
    skip_wl_campaigns: bool = True,
    global_min_campaigns: int = 2,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """
    For each **global** WL source (on ≥ ``global_min_campaigns`` campaign API whitelists),
    list campaigns where the source is on the **merged** WL (campaign ∪ sheet for geo) and
    ``adv-stats-by-source`` shows **0 clicks** in the window — bid-test candidates.
    """
    sess = session or requests.Session()
    pairs = global_whitelist_sources(list(campaigns), min_campaigns=global_min_campaigns)
    global_sources = [s for s, _ in pairs]
    utc_today = datetime.now(timezone.utc).date()
    start, end = date_range_last_days(utc_today, max(1, min(int(days), 90)))
    by_source: Dict[str, List[Dict[str, Any]]] = {s: [] for s in global_sources}
    errors: List[str] = []

    for c in campaigns:
        if not isinstance(c, dict):
            continue
        nm = str(c.get("name") or "")
        if skip_wl_campaigns and _campaign_suffix_wl(nm):
            continue
        cid = str(c.get("id") or "")
        if not cid:
            continue
        merged, _fc, _fs = merged_whitelist_for_campaign(c, geo_map)
        on_here = [s for s in global_sources if merged_contains_source(merged, s)]
        if not on_here:
            continue
        try:
            stats = fetch_adv_stats_by_source(
                cid, start, end, advertiser_key, auth_key, secret_key, session=sess
            )
        except Exception as e:
            errors.append(f"{nm}: {e}")
            continue
        for s in on_here:
            if clicks_for_source_from_stats(stats, s) != 0:
                continue
            db = campaign_default_bid_value(c)
            sc = cpc_for_source_from_campaign(c, s)
            by_source[s].append(
                {
                    "campaign_id": cid,
                    "campaign_name": nm,
                    "geo": normalize_geo_key(str(c.get("geo") or "")),
                    "default_bid": db,
                    "source_cpc": sc,
                    "clicks": 0,
                }
            )

    for s in by_source:
        by_source[s].sort(key=lambda x: (str(x.get("campaign_name") or "").lower(), x.get("campaign_id") or ""))

    return {
        "stats_window_start": start,
        "stats_window_end": end,
        "days": int(days),
        "by_source": by_source,
        "errors": errors[:80],
    }


def build_cpcbysource_with_source_cpc(
    full_campaign: Mapping[str, Any],
    source: str,
    new_cpc: float,
) -> Dict[str, Any]:
    """Copy ``cpcbysource``, drop prior keys for this source (any casing), set ``source`` → ``new_cpc``."""
    raw = full_campaign.get("cpcbysource")
    out: Dict[str, Any] = {}
    canon = (source or "").strip()
    cl = canon.lower()
    if isinstance(raw, dict):
        for k, v in raw.items():
            ks = str(k).strip()
            if ks.lower() == cl:
                continue
            out[ks] = v
    out[canon] = float(new_cpc)
    return out


def apply_wl_potential_cpcbysource_updates(
    advertiser_key: str,
    auth_key: str,
    secret_key: str,
    *,
    source: str,
    campaign_ids: Sequence[str],
    new_cpc: float,
    dry_run: bool = False,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """Set ``cpcbysource[source]`` on each campaign (full GET → merge → POST)."""
    sess = session or requests.Session()
    log: List[Dict[str, Any]] = []
    errors: List[str] = []
    seen = {str(x).strip() for x in campaign_ids if str(x).strip()}
    for cid in seen:
        try:
            full = fetch_campaign_by_id(cid, advertiser_key, auth_key, secret_key, session=sess)
        except Exception as e:
            errors.append(f"{cid}: fetch {e}")
            continue
        if not full:
            errors.append(f"{cid}: empty campaign")
            continue
        payload = dict(full)
        payload["cpcbysource"] = build_cpcbysource_with_source_cpc(full, source, new_cpc)
        cname = str(full.get("name") or cid)
        if dry_run:
            log.append(
                {
                    "campaign_id": cid,
                    "campaign_name": cname,
                    "dry_run": True,
                    "cpcbysource": payload["cpcbysource"],
                }
            )
            continue
        try:
            post_update_advertiser_campaign(cid, payload, advertiser_key, auth_key, secret_key, session=sess)
            log.append({"campaign_id": cid, "campaign_name": cname, "ok": True})
        except Exception as e:
            errors.append(f"{cname}: {e}")
        time.sleep(0.35)
    if dry_run:
        for entry in log:
            logger.info("WL potential dry-run: %s", entry)
    return {"ok": not errors, "dry_run": dry_run, "log": log, "errors": errors, "source": source, "new_cpc": new_cpc}


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
        wl, wl_camp, wl_sheet = merged_whitelist_for_campaign(c, geo_map)
        if not wl:
            summaries.append(
                {
                    "campaign_id": str(c.get("id") or ""),
                    "campaign_name": nm,
                    "geo": geo,
                    "note": "no_whitelist_campaign_or_sheet",
                    "whitelist_size": 0,
                    "whitelist_from_campaign": [],
                    "whitelist_from_sheet": wl_sheet,
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
        aud["whitelist_from_campaign"] = wl_camp
        aud["whitelist_from_sheet"] = wl_sheet
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
                    "source_on_campaign_wl": w in set(wl_camp),
                    "source_on_sheet_wl": w in set(wl_sheet),
                }
            )
    return flat, summaries


def whitelist_focus_source_traffic_no_buy(
    advertiser_key: str,
    auth_key: str,
    secret_key: str,
    geo_map: Mapping[str, Mapping[str, Any]],
    focus_source: str,
    *,
    days: int = 30,
    skip_wl_campaigns: bool = True,
    limit_campaigns: int = 0,
    min_campaign_matches: int = 2,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """
    For one source: campaigns where it is on the **merged** whitelist, had **clicks > 0**
    in ``adv-stats-by-source`` for the window, and **conversions/sales** from that row are 0
    (see ``conversions_from_source_stat`` — omitted API fields count as 0).

    ``min_campaign_matches``: if the number of matching campaigns is below this, ``rows`` is
    empty (useful to only surface cross-campaign patterns, e.g. 2+).
    """
    src = (focus_source or "").strip()
    try:
        d_days = max(1, min(int(days), 90))
    except (TypeError, ValueError):
        d_days = 30
    try:
        min_m_cfg = max(1, int(min_campaign_matches))
    except (TypeError, ValueError):
        min_m_cfg = 2
    out: Dict[str, Any] = {
        "source": src,
        "days": d_days,
        "stats_window_start": "",
        "stats_window_end": "",
        "rows": [],
        "errors": [],
        "min_campaign_matches": min_m_cfg,
        "campaigns_on_whitelist": 0,
    }
    if not src:
        out["error"] = "empty_source"
        return out

    sess = session or requests.Session()
    campaigns = fetch_campaigns(advertiser_key, auth_key, secret_key, session=sess)
    utc_today = datetime.now(timezone.utc).date()
    start, end = date_range_last_days(utc_today, d_days)
    out["stats_window_start"] = start
    out["stats_window_end"] = end

    rows_acc: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    on_wl = 0
    n = 0
    src_l = src.lower()

    def _focus_in_sources(seq: Sequence[str]) -> bool:
        for w in seq:
            t = str(w).strip()
            if not t:
                continue
            if t == src or t.lower() == src_l:
                return True
        return False

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
        wl, wl_camp, wl_sheet = merged_whitelist_for_campaign(c, geo_map)
        wl_norm = {str(x).strip() for x in wl if str(x).strip()}
        if src not in wl_norm and not any(w.lower() == src_l for w in wl_norm):
            continue
        on_wl += 1

        cid = str(c.get("id") or "")
        try:
            stats = fetch_adv_stats_by_source(
                cid,
                start,
                end,
                advertiser_key,
                auth_key,
                secret_key,
                session=sess,
            )
        except Exception as e:
            errors.append(
                {
                    "campaign_id": cid,
                    "campaign_name": nm,
                    "geo": geo,
                    "error": str(e),
                }
            )
            continue

        st_row = find_adv_stats_row_for_source(stats, src)
        if not st_row:
            continue
        try:
            clicks = int(st_row.get("clicks") or 0)
        except (TypeError, ValueError):
            clicks = 0
        conv = conversions_from_source_stat(st_row)
        conv_known = conversions_field_present_in_stat(st_row)
        if clicks <= 0:
            continue
        if conv > 0:
            continue
        rows_acc.append(
            {
                "campaign_id": cid,
                "campaign_name": nm,
                "geo": geo,
                "source": str(st_row.get("source") or src).strip() or src,
                "clicks": clicks,
                "conversions": conv,
                "conversions_field_present": conv_known,
                "source_on_campaign_wl": _focus_in_sources([str(x) for x in wl_camp]),
                "source_on_sheet_wl": _focus_in_sources([str(x) for x in wl_sheet]),
            }
        )

    out["campaigns_on_whitelist"] = on_wl
    out["raw_match_count"] = len(rows_acc)
    out["errors"] = errors
    min_m = min_m_cfg
    if len(rows_acc) >= min_m:
        out["rows"] = rows_acc
    else:
        out["rows"] = []
        out["below_min_matches"] = len(rows_acc)
    out["shown_match_count"] = len(out["rows"])
    return out


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
