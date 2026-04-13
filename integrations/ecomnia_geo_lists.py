"""
Ecomnia per-geo blacklist aggregation + whitelist traffic audit (adv-stats-by-source).

Mirrors the intent of ``tools/ec_local_copy.py`` (``explorations_blacklist_synch``,
``potentialSources30days``) using ``config`` credentials and the reporting API.

- **Geo blacklist (recommended):** count, per source × geo, how many campaigns in that
  geo already blacklist the source; sources at or above ``min_blacklist_hits_in_geo``
  are candidates for a shared geo blacklist (legacy default: more than 3 → 4+ hits).

- **30-day WL audit:** for each campaign, compare ``adv-stats-by-source`` against the
  geo whitelist from your sheet (``globaList``). EC returns sources with 0 clicks too;
  those appear as ``clicks == 0`` or missing from the map → treated as 0.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import date, datetime, timedelta, timezone
from collections import Counter
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import requests

from config import ECOMNIA_REPORT_BASE

logger = logging.getLogger(__name__)

EC_ADVERTISER_BASE = "https://advertiser.ecomnia.com"


def authtoken_now(secret_key: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    return hashlib.md5((ts + (secret_key or "")).encode("utf-8")).hexdigest().upper()


def authtoken_range(secret_key: str, start: str, end: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    return hashlib.md5(f"{ts}{start}{end}{secret_key or ''}".encode("utf-8")).hexdigest().upper()


def _params_now(advertiser_key: str, auth_key: str, secret_key: str) -> Dict[str, str]:
    return {
        "advertiserkey": advertiser_key or "",
        "authkey": auth_key or "",
        "authtoken": authtoken_now(secret_key),
    }


def _params_range(
    advertiser_key: str, auth_key: str, secret_key: str, start: str, end: str
) -> Dict[str, str]:
    return {
        "advertiserkey": advertiser_key or "",
        "authkey": auth_key or "",
        "authtoken": authtoken_range(secret_key, start, end),
    }


def fetch_campaigns(
    advertiser_key: str,
    auth_key: str,
    secret_key: str,
    *,
    session: Optional[requests.Session] = None,
    timeout: float = 120.0,
) -> List[Dict[str, Any]]:
    url = f"{EC_ADVERTISER_BASE}/get-advertiser-campaigns"
    sess = session or requests.Session()
    r = sess.get(url, params=_params_now(advertiser_key, auth_key, secret_key), timeout=timeout)
    r.raise_for_status()
    data = r.json() if r.text else {}
    rows = data.get("campaigns") if isinstance(data, dict) else None
    return [c for c in rows if isinstance(c, dict)] if isinstance(rows, list) else []


def fetch_campaign_by_id(
    campaign_id: str,
    advertiser_key: str,
    auth_key: str,
    secret_key: str,
    *,
    session: Optional[requests.Session] = None,
    timeout: float = 120.0,
) -> Optional[Dict[str, Any]]:
    url = f"{EC_ADVERTISER_BASE}/get-advertiser-campaigns"
    sess = session or requests.Session()
    params = dict(_params_now(advertiser_key, auth_key, secret_key))
    params["campaign_id"] = campaign_id
    r = sess.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json() if r.text else {}
    rows = data.get("campaigns") if isinstance(data, dict) else None
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        return rows[0]
    return None


def fetch_adv_stats_by_source(
    campaign_id: str,
    start_date: str,
    end_date: str,
    advertiser_key: str,
    auth_key: str,
    secret_key: str,
    *,
    report_base: Optional[str] = None,
    session: Optional[requests.Session] = None,
    timeout: float = 120.0,
) -> List[Dict[str, Any]]:
    base = (report_base or ECOMNIA_REPORT_BASE or "https://report.ecomnia.com").rstrip("/")
    url = f"{base}/adv-stats-by-source"
    sess = session or requests.Session()
    params = dict(
        _params_range(advertiser_key, auth_key, secret_key, start_date, end_date),
        startdate=start_date,
        enddate=end_date,
        campaignid=campaign_id,
    )
    r = sess.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json() if r.text else {}
    stats = data.get("stats") if isinstance(data, dict) else None
    return [s for s in stats if isinstance(s, dict)] if isinstance(stats, list) else []


def fetch_adv_stats_by_date(
    campaign_id: str,
    start_date: str,
    end_date: str,
    advertiser_key: str,
    auth_key: str,
    secret_key: str,
    *,
    report_base: Optional[str] = None,
    session: Optional[requests.Session] = None,
    timeout: float = 120.0,
) -> List[Dict[str, Any]]:
    """GET ``adv-stats-by-date`` — daily spend/clicks rows for the campaign."""
    base = (report_base or ECOMNIA_REPORT_BASE or "https://report.ecomnia.com").rstrip("/")
    url = f"{base}/adv-stats-by-date"
    sess = session or requests.Session()
    params = dict(
        _params_range(advertiser_key, auth_key, secret_key, start_date, end_date),
        startdate=start_date,
        enddate=end_date,
        campaignid=campaign_id,
    )
    r = sess.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json() if r.text else {}
    stats = data.get("stats") if isinstance(data, dict) else None
    return [s for s in stats if isinstance(s, dict)] if isinstance(stats, list) else []


def campaign_daily_budget_value(campaign: Mapping[str, Any]) -> Optional[float]:
    """Best-effort daily budget from campaign object (API field names vary)."""
    for k in ("dailybudget", "daily_budget", "dailyBudget"):
        v = campaign.get(k)
        if v is None or v == "":
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return None


def spend_on_date_from_daily_stats(
    daily_rows: Sequence[Mapping[str, Any]], ymd: str
) -> float:
    """Sum ``spend`` for rows whose ``date`` matches ``YYYY-MM-DD`` (prefix match)."""
    target = (ymd or "").strip()[:10]
    total = 0.0
    for row in daily_rows:
        d = str(row.get("date") or "")[:10]
        if d != target:
            continue
        try:
            total += float(row.get("spend") or 0)
        except (TypeError, ValueError):
            continue
    return total


def parse_source_list_cell(raw: Any) -> List[str]:
    """Parse sheet cell: JSON array, or comma / newline / semicolon separated names."""
    if raw is None:
        return []
    s = str(raw).strip()
    if not s:
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            val = json.loads(s)
            if isinstance(val, list):
                return [str(x).strip() for x in val if str(x).strip()]
        except json.JSONDecodeError:
            pass
    parts = re.split(r"[,;\n\r]+", s)
    return [p.strip() for p in parts if p.strip()]


def geo_bw_map_from_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    geo_col: str = "geo",
    blacklist_col: str = "blacklist",
    whitelist_col: str = "whitelist",
) -> Dict[str, Dict[str, List[str]]]:
    """
    Build ``{ geo_lower: {"blacklist": [...], "whitelist": [...]} }`` from sheet dict rows
    (keys matched case-insensitively on first row's logic — caller should normalize keys to lower).
    """
    out: Dict[str, Dict[str, List[str]]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        geo = ""
        bl_raw = ""
        wl_raw = ""
        for k, v in row.items():
            kl = str(k).strip().lower()
            if kl == geo_col.lower():
                geo = str(v or "").strip().lower()
            elif kl == blacklist_col.lower():
                bl_raw = v
            elif kl == whitelist_col.lower():
                wl_raw = v
        if not geo:
            continue
        out[geo] = {
            "blacklist": parse_source_list_cell(bl_raw),
            "whitelist": parse_source_list_cell(wl_raw),
        }
    return out


def normalize_geo_key(geo: str) -> str:
    g = (geo or "").strip().lower()
    if g == "gb":
        return "uk"
    return g


def campaign_whitelist_sources(campaign: Mapping[str, Any]) -> List[str]:
    """
    Traffic sources whitelisted on this campaign (Ecomnia ``whitelistsources``).
    Tries common API key spellings.
    """
    for key in ("whitelistsources", "whitelistSources", "whitelist_sources"):
        wl = campaign.get(key)
        if isinstance(wl, list):
            return sorted({str(x).strip() for x in wl if str(x).strip()})
    return []


def aggregate_whitelist_source_campaign_counts(
    campaigns: Sequence[Mapping[str, Any]],
) -> Dict[str, int]:
    """For each source id, how many distinct campaigns include it in ``whitelistsources``."""
    ctr: Counter[str] = Counter()
    for c in campaigns:
        if not isinstance(c, Mapping):
            continue
        for s in campaign_whitelist_sources(c):
            ctr[s] += 1
    return dict(ctr)


def global_whitelist_sources(
    campaigns: Sequence[Mapping[str, Any]],
    *,
    min_campaigns: int = 2,
) -> List[Tuple[str, int]]:
    """
    **Global WL:** sources that appear on the whitelist of at least ``min_campaigns`` campaigns.
    Returns ``(source, campaign_count)`` sorted by count desc, then source name.
    """
    counts = aggregate_whitelist_source_campaign_counts(campaigns)
    pairs = [(s, n) for s, n in counts.items() if n >= min_campaigns]
    pairs.sort(key=lambda x: (-x[1], x[0]))
    return pairs


def whitelist_union_by_geo_from_campaigns(
    campaigns: Sequence[Mapping[str, Any]],
) -> Dict[str, List[str]]:
    """Union of each campaign's ``whitelistsources``, grouped by that campaign's geo."""
    by_geo: Dict[str, set[str]] = {}
    for c in campaigns:
        if not isinstance(c, Mapping):
            continue
        geo = normalize_geo_key(str(c.get("geo") or ""))
        if not geo:
            continue
        for s in campaign_whitelist_sources(c):
            by_geo.setdefault(geo, set()).add(s)
    return {g: sorted(v) for g, v in sorted(by_geo.items(), key=lambda x: x[0])}


def merged_whitelist_for_campaign(
    campaign: Mapping[str, Any],
    geo_map: Mapping[str, Mapping[str, Any]],
) -> Tuple[List[str], List[str], List[str]]:
    """
    Returns ``(merged, from_campaign, from_sheet)`` — union of API campaign WL and sheet geo WL.
    """
    geo = normalize_geo_key(str(campaign.get("geo") or ""))
    from_c = campaign_whitelist_sources(campaign)
    from_sheet: List[str] = []
    gentry = geo_map.get(geo) or {}
    if isinstance(gentry, dict):
        raw = gentry.get("whitelist")
        if isinstance(raw, list):
            from_sheet = [str(x).strip() for x in raw if str(x).strip()]
    merged = sorted(set(from_c) | set(from_sheet))
    return merged, from_c, from_sheet


def aggregate_source_blacklist_counts_by_geo(
    campaigns: Sequence[Mapping[str, Any]],
) -> Dict[str, Dict[str, int]]:
    """
    For each source name, count how many campaigns (per geo) include it in ``blacklistsources``.
    Returns ``source -> { geo: count }``.
    """
    counts: Dict[str, Dict[str, int]] = {}
    for c in campaigns:
        if not isinstance(c, Mapping):
            continue
        geo = normalize_geo_key(str(c.get("geo") or ""))
        if not geo:
            continue
        bl = c.get("blacklistsources")
        if not isinstance(bl, list):
            continue
        for src in bl:
            name = str(src).strip()
            if not name:
                continue
            if name not in counts:
                counts[name] = {}
            counts[name][geo] = counts[name].get(geo, 0) + 1
    return counts


def recommended_geo_blacklists(
    campaigns: Sequence[Mapping[str, Any]],
    *,
    min_hits_in_geo: int = 4,
    min_total_hits_across_geos: int = 5,
) -> Tuple[Dict[str, List[str]], List[str]]:
    """
    Legacy rules from ``explorations_blacklist_synch``:
    - If a source is blacklisted in **more than 3** campaigns in the same geo → add to that geo list
      (``min_hits_in_geo`` default **4** = strictly more than 3).
    - If sum of per-geo counts **> 5** → global candidate (default threshold: ``total > 5`` i.e. 6+).
    """
    per_source = aggregate_source_blacklist_counts_by_geo(campaigns)
    by_geo: Dict[str, List[str]] = {}
    global_candidates: List[str] = []

    for source, geo_counts in per_source.items():
        total = sum(geo_counts.values())
        if total > min_total_hits_across_geos:
            global_candidates.append(source)
        for geo, n in geo_counts.items():
            if n >= min_hits_in_geo:
                by_geo.setdefault(geo, []).append(source)

    for geo in by_geo:
        by_geo[geo] = sorted(set(by_geo[geo]))
    global_candidates = sorted(set(global_candidates))
    return by_geo, global_candidates


def clicks_by_source_from_stats(stats: Sequence[Mapping[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for row in stats:
        name = str(row.get("source") or "").strip()
        if not name:
            continue
        try:
            clicks = int(row.get("clicks") or 0)
        except (TypeError, ValueError):
            clicks = 0
        out[name] = clicks
    return out


def find_adv_stats_row_for_source(
    stats: Sequence[Mapping[str, Any]], target: str
) -> Optional[Dict[str, Any]]:
    """Return the ``adv-stats-by-source`` row for ``target`` (exact ``source``, then case-insensitive)."""
    t = (target or "").strip()
    if not t:
        return None
    for row in stats:
        if not isinstance(row, dict):
            continue
        name = str(row.get("source") or "").strip()
        if name == t:
            return dict(row)
    tl = t.lower()
    for row in stats:
        if not isinstance(row, dict):
            continue
        name = str(row.get("source") or "").strip()
        if name.lower() == tl:
            return dict(row)
    return None


def conversions_from_source_stat(row: Mapping[str, Any]) -> int:
    """
    Best-effort conversions / sales count from one ``adv-stats-by-source`` row.
    If the API omits these fields, returns ``0`` (callers should treat as “no signal”).
    """
    lk = {str(k).lower(): v for k, v in row.items()}
    for key in (
        "conversions",
        "conversion",
        "sales",
        "salecount",
        "orders",
        "purchases",
        "convertedclicks",
        "conv",
    ):
        if key not in lk:
            continue
        v = lk[key]
        if isinstance(v, bool):
            continue
        try:
            return int(float(v))
        except (TypeError, ValueError):
            continue
    return 0


def conversions_field_present_in_stat(row: Mapping[str, Any]) -> bool:
    lk = {str(k).lower() for k in row}
    return bool(
        lk.intersection(
            {
                "conversions",
                "conversion",
                "sales",
                "salecount",
                "orders",
                "purchases",
                "convertedclicks",
                "conv",
            }
        )
    )


def audit_whitelist_traffic(
    campaign: Mapping[str, Any],
    geo_whitelist: Sequence[str],
    stats_by_source: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """
    Per campaign: which whitelist sources had clicks > 0 in the adv-stats-by-source window.
    """
    geo = normalize_geo_key(str(campaign.get("geo") or ""))
    cid = str(campaign.get("id") or "")
    cname = str(campaign.get("name") or "")
    clicks_map = clicks_by_source_from_stats(stats_by_source)
    wl = [str(s).strip() for s in geo_whitelist if str(s).strip()]
    per_wl: List[Dict[str, Any]] = []
    for w in wl:
        n = int(clicks_map.get(w, 0))
        per_wl.append({"source": w, "clicks": n, "had_traffic": n > 0})
    with_clicks = [x["source"] for x in per_wl if x["had_traffic"]]
    zero = [x["source"] for x in per_wl if not x["had_traffic"]]
    return {
        "campaign_id": cid,
        "campaign_name": cname,
        "geo": geo,
        "whitelist_size": len(wl),
        "whitelist_sources_with_clicks": with_clicks,
        "whitelist_sources_zero_clicks": zero,
        "any_whitelist_click": bool(with_clicks),
        "sources_in_report": len(clicks_map),
    }


def date_range_last_days(utc_today: date, days: int) -> Tuple[str, str]:
    """Inclusive-ish window [today-days, today] as YYYY-MM-DD (same as legacy ~31 days for '30d')."""
    end = utc_today.isoformat()
    start = (utc_today - timedelta(days=days)).isoformat()
    return start, end


def post_update_advertiser_campaign(
    campaign_id: str,
    campaign_payload: Dict[str, Any],
    advertiser_key: str,
    auth_key: str,
    secret_key: str,
    *,
    session: Optional[requests.Session] = None,
    timeout: float = 120.0,
) -> Dict[str, Any]:
    """POST ``update-advertiser-campaign`` with full campaign JSON (same as legacy ``update_campaign``)."""
    url = f"{EC_ADVERTISER_BASE}/update-advertiser-campaign"
    sess = session or requests.Session()
    params = dict(_params_now(advertiser_key, auth_key, secret_key))
    params["id"] = campaign_id
    r = sess.post(
        url,
        params=params,
        json=campaign_payload,
        headers={"Content-Type": "application/json"},
        timeout=timeout,
    )
    r.raise_for_status()
    try:
        return r.json() if r.text else {}
    except Exception:
        return {"raw": r.text[:500]}


def merge_recommended_into_geo_map(
    geo_map: Dict[str, Dict[str, List[str]]],
    recommended_by_geo: Mapping[str, Sequence[str]],
) -> Dict[str, Dict[str, List[str]]]:
    """Return new map: blacklist = sorted union of sheet + recommended per geo."""
    out = {g: {"blacklist": list(v.get("blacklist", [])), "whitelist": list(v.get("whitelist", []))} for g, v in geo_map.items()}
    for geo, sources in recommended_by_geo.items():
        g = normalize_geo_key(geo)
        bucket = out.setdefault(g, {"blacklist": [], "whitelist": []})
        merged = set(bucket["blacklist"]) | set(sources)
        bucket["blacklist"] = sorted(merged)
    return out
