"""
Append EC publisher sources to ``trackExploration.wl`` from Keitaro sale conversions.

Keitaro ``conversions/log`` rows with ``sub_id_6`` ending in ``-EC`` carry:
  - ``sub_id_6`` — ``{brand}-{geo}-{prefix}-EC`` (affiliation / brand tag)
  - ``sub_id_5`` — EC source id (same values as sheet ``wl`` / campaign lists)

Statuses scanned: ``SaleOur``, ``LateSale`` (default lookback 30 days).

New sources are appended at the end of the sheet ``wl`` list and to campaign
``whitelistsources``. Blacklisted sources are reactivated via absolute
``cpcbysource`` bid (default $0.10) — EC has no bidFactor.

Also appends missing ``ECQualityWL`` rows (``CampaignID`` × ``SUBID``).
"""
from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from config import (
    EC_EXPLORATION_WL_LOOKBACK_DAYS,
    EC_EXPLORATION_WL_REACTIVATE_TARGET_BID_USD,
    EC_SHEETS_SPREADSHEET_ID,
)
from integrations.autoserver import gdocs_as as gd
from integrations.autoserver.ec import (
    HEADERS_EXPLORATION,
    TAB_EXPLORATION,
    activate_campaignWitId,
    get_campaignById,
    get_campaigns_statsBySource,
    reactivate_sources_ec,
    whiteListSources,
)
from integrations.autoserver.sk_optimizer import _append_logs_cell, _format_wl, _parse_wl
from integrations.keitaro import KeitaroClient, KeitaroClientError
from integrations.keitaro_conversions import iter_conversion_log

logger = logging.getLogger(__name__)

_EC_SUBID6_RE = re.compile(r"^(.+)-([A-Za-z]{2})-.+-EC$", re.IGNORECASE)
_EC_SUBID6_FULL_RE = re.compile(r"^(.+)-([A-Za-z]{2})-(.+)-EC$", re.IGNORECASE)
_EC_CAMPAIGN_PREFIX_RE = re.compile(r"(KLFIX|KLFLEX|KLTESTED|KLWL\d*)", re.IGNORECASE)

TAB_QUALITY_WL = "ECQualityWL"
HEADERS_QUALITY_WL = [
    "",
    "CampaignID",
    "SUBID",
    "ECstatus",
    "clicks30",
    "clicks7",
    "clicksYest",
    "clicksToday",
    "bid",
    "url",
    "lastUpdate",
]

_SALE_STATUSES = ("SaleOur", "LateSale")
_EC_LOG_COLUMNS = ("sub_id_5", "sub_id_6", "status", "datetime")
_STATE_PATH = Path(__file__).resolve().parents[2] / "data" / "ec_exploration_wl_sync_state.json"


def _norm_brand_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").strip().lower())


def _norm_geo(value: str) -> str:
    g = (value or "").strip().lower()[:2]
    if g == "gb":
        return "uk"
    return g


def _quality_wl_geo_label(geo: str) -> str:
    g = _norm_geo(geo)
    return "UK" if g == "uk" else g.upper()


def _quality_wl_label_from_sub_id_6(sub_id_6: str, brand: str, geo: str, campaign_name: str) -> str:
    m = _EC_SUBID6_FULL_RE.match((sub_id_6 or "").strip())
    if m:
        brand_key = _norm_brand_key(m.group(1))
        geo_up = _quality_wl_geo_label(_norm_geo(m.group(2)))
        prefix = m.group(3)
        return f"{brand_key}-{geo_up}-{prefix}"
    return _quality_wl_label(brand, geo, campaign_name)


def _quality_wl_label(brand: str, geo: str, campaign_name: str) -> str:
    brand_key = _norm_brand_key(brand)
    if not brand_key and campaign_name:
        brand_key = _norm_brand_key(campaign_name.split("-", 1)[0])
    geo_up = _quality_wl_geo_label(geo)
    prefix = "KLFIX"
    m = _EC_CAMPAIGN_PREFIX_RE.search(campaign_name or "")
    if m:
        prefix = m.group(1).upper()
    return f"{brand_key}-{geo_up}-{prefix}"


def parse_ec_sub_id_6(sub_id_6: str) -> Optional[Tuple[str, str]]:
    """Return ``(brand_key, geo)`` from ``brand-geo-prefix-EC`` or ``None``."""
    m = _EC_SUBID6_RE.match((sub_id_6 or "").strip())
    if not m:
        return None
    return _norm_brand_key(m.group(1)), _norm_geo(m.group(2))


def _brand_geo_from_camp_name(camp_name: str) -> Optional[Tuple[str, str]]:
    """Parse ``{brand}-{geo}-{prefix}`` (geo may be GB for UK)."""
    name = (camp_name or "").strip()
    if not name or "-" not in name:
        return None
    parts = name.split("-")
    if len(parts) < 3:
        return None
    geo = _norm_geo(parts[-2])
    brand = _norm_brand_key("-".join(parts[:-2]))
    if not brand or not geo:
        return None
    return brand, geo


def _exploration_index(rows: List[Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """``(norm_brand, geo)`` -> first matching exploration row."""
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in rows:
        brand = _norm_brand_key(str(row.get("brand") or ""))
        geo = _norm_geo(str(row.get("geo") or ""))
        if not brand or not geo:
            parsed = _brand_geo_from_camp_name(str(row.get("campName") or ""))
            if parsed:
                brand, geo = parsed
        if not brand or not geo:
            continue
        key = (brand, geo)
        if key not in out:
            out[key] = row
    return out


def _blank_quality_wl_row(label: str, campaign_id: str, sub_id: str) -> Dict[str, str]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    cid = str(campaign_id).strip()
    sub = str(sub_id).strip()
    return {
        "": label,
        "CampaignID": cid,
        "SUBID": sub,
        "ECstatus": "",
        "clicks30": "",
        "clicks7": "",
        "clicksYest": "",
        "clicksToday": "",
        "bid": "",
        "url": "",
        "lastUpdate": now,
    }


def _quality_wl_existing_keys(rows: List[Dict[str, Any]]) -> Set[Tuple[str, str]]:
    out: Set[Tuple[str, str]] = set()
    for row in rows:
        cid = str(row.get("CampaignID") or "").strip()
        sub = str(row.get("SUBID") or "").strip()
        if cid and sub:
            out.add((cid, sub))
    return out


def ensure_ec_quality_wl_sheet(sheet_id: Optional[str] = None) -> str:
    sid = (sheet_id or EC_SHEETS_SPREADSHEET_ID or "").strip()
    if not sid:
        raise RuntimeError("EC_SHEETS_SPREADSHEET_ID is not configured")
    gd.append_missing_headers_row1(sid, TAB_QUALITY_WL, HEADERS_QUALITY_WL, create_if_missing=True)
    return sid


def append_ec_quality_wl_rows(
    candidates: List[Tuple[str, str, str]],
    *,
    dry_run: bool = False,
    sheet_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Append ``ECQualityWL`` rows for ``(campaign_id, sub_id, label)``; skip existing pairs."""
    if not candidates:
        return {
            "dry_run": dry_run,
            "appended": 0,
            "skipped_existing": 0,
            "quality_wl_rows_before": 0,
            "quality_wl_rows_after": 0,
            "details": [],
        }

    sid = ensure_ec_quality_wl_sheet(sheet_id)
    sheet = gd.read_sheet_withID(sid, TAB_QUALITY_WL) or []
    existing = _quality_wl_existing_keys(sheet)
    appended_rows: List[Dict[str, str]] = []
    skipped_existing = 0
    seen_new: Set[Tuple[str, str]] = set()

    for campaign_id, sub_id, label in candidates:
        cid = str(campaign_id or "").strip()
        sub = str(sub_id or "").strip()
        if not cid or not sub:
            continue
        key = (cid, sub)
        if key in existing or key in seen_new:
            skipped_existing += 1
            continue
        seen_new.add(key)
        appended_rows.append(_blank_quality_wl_row(label, cid, sub))

    if appended_rows and not dry_run:
        headers = list(sheet[0].keys()) if sheet else list(HEADERS_QUALITY_WL)
        for h in HEADERS_QUALITY_WL:
            if h not in headers:
                headers.append(h)
        merged: List[Dict[str, str]] = []
        for row in sheet:
            merged.append({h: "" if row.get(h) is None else str(row.get(h, "")) for h in headers})
        for row in appended_rows:
            merged.append({h: "" if row.get(h) is None else str(row.get(h, "")) for h in headers})
        gd.create_or_update_sheet_from_dicts_withId(sid, TAB_QUALITY_WL, merged)

    return {
        "dry_run": dry_run,
        "appended": len(appended_rows),
        "skipped_existing": skipped_existing,
        "quality_wl_rows_before": len(sheet),
        "quality_wl_rows_after": len(sheet) + len(appended_rows),
        "details": [
            {"campaign_id": r["CampaignID"], "sub_id": r["SUBID"], "label": r.get("", "")}
            for r in appended_rows[:50]
        ],
    }


def collect_ec_sale_sources_detailed(
    *,
    lookback_days: Optional[int] = None,
    client: Optional[KeitaroClient] = None,
) -> Dict[Tuple[str, str], List[Tuple[str, str]]]:
    """Map ``(norm_brand, geo)`` -> ordered ``(sub_id_5, sub_id_6)`` pairs (first-seen)."""
    days = max(1, int(lookback_days or EC_EXPLORATION_WL_LOOKBACK_DAYS))
    c = client or KeitaroClient()
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=days)

    order: Dict[Tuple[str, str], Dict[str, Tuple[int, str]]] = defaultdict(dict)
    seq = 0

    for status in _SALE_STATUSES:
        try:
            for row in iter_conversion_log(
                c,
                date_from=start,
                date_to=today,
                status=status,
                columns=list(_EC_LOG_COLUMNS),
            ):
                sub5 = str(row.get("sub_id_5") or "").strip()
                sub6 = str(row.get("sub_id_6") or "").strip()
                parsed = parse_ec_sub_id_6(sub6)
                if not sub5 or not parsed:
                    continue
                if sub5 not in order[parsed]:
                    order[parsed][sub5] = (seq, sub6)
                    seq += 1
        except KeitaroClientError as e:
            logger.warning("EC WL sync: Keitaro conversions/log status=%s failed: %s", status, e)

    out: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
    for key, subs in order.items():
        out[key] = [
            (sub, sub6)
            for sub, (_, sub6) in sorted(subs.items(), key=lambda kv: kv[1][0])
        ]
    return out


def _camp_payload(camp_id: str) -> Optional[dict]:
    try:
        block = get_campaignById(camp_id)
        camps = block.get("campaigns") if isinstance(block, dict) else None
        if isinstance(camps, list) and camps and isinstance(camps[0], dict):
            return camps[0]
    except Exception as e:
        logger.warning("EC WL sync: get_campaignById(%s) failed: %s", camp_id, e)
    return None


def _subs_needing_reactivation_ec(
    camp_data: Optional[dict],
    sale_subs: List[str],
    wl_set: Set[str],
    to_add: List[str],
) -> List[str]:
    """New WL subs + converting / WL subs still on blacklist or with non-positive CPC."""
    need: Set[str] = set(to_add)
    if not camp_data:
        return sorted(need)
    bl = {str(x).strip() for x in (camp_data.get("blacklistsources") or []) if str(x).strip()}
    cpc_raw = camp_data.get("cpcbysource") or {}
    if not isinstance(cpc_raw, dict):
        cpc_raw = {}
    for sid in sale_subs:
        if sid in need:
            continue
        if sid in bl:
            need.add(sid)
            continue
        if sid in wl_set:
            try:
                val = float(cpc_raw.get(sid)) if cpc_raw.get(sid) is not None else None
            except (TypeError, ValueError):
                val = None
            if val is not None and val <= 0:
                need.add(sid)
    return sorted(need)


def _load_sync_state() -> Dict[str, str]:
    if not _STATE_PATH.exists():
        return {}
    try:
        data = json.loads(_STATE_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
    except Exception as e:
        logger.warning("EC exploration WL sync state read failed: %s", e)
    return {}


def _save_sync_state(state: Dict[str, str]) -> None:
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def wl_sync_already_ran_today(today: Optional[str] = None) -> bool:
    day = today or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return _load_sync_state().get("last_run_date") == day


def mark_wl_sync_done(today: Optional[str] = None) -> None:
    day = today or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    state = _load_sync_state()
    state["last_run_date"] = day
    state["last_run_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _save_sync_state(state)


def _write_exploration_wl_patches(
    sheet_id: str,
    patches: Dict[str, Dict[str, str]],
) -> None:
    """Merge ``wl`` (and optional log fields) onto a fresh sheet read, then rewrite."""
    if not patches:
        return
    gd.append_missing_headers_row1(sheet_id, TAB_EXPLORATION, HEADERS_EXPLORATION, create_if_missing=False)
    fresh = gd.read_sheet_withID(sheet_id, TAB_EXPLORATION) or []
    changed = False
    for row in fresh:
        cid = str(row.get("campId") or "").strip()
        name = str(row.get("campName") or "").strip().lower()
        patch = patches.get(cid) or patches.get(f"name:{name}")
        if not patch:
            continue
        for key, value in patch.items():
            if str(row.get(key) or "") != str(value or ""):
                row[key] = value
                changed = True
    if not changed:
        return
    # Preserve list-ish cells as strings for Sheets.
    for row in fresh:
        for listish in ("wl", "verify"):
            v = row.get(listish)
            if isinstance(v, (list, dict)):
                row[listish] = json.dumps(v, ensure_ascii=False)
    gd.create_or_update_sheet_from_dicts_withId(sheet_id, TAB_EXPLORATION, fresh)


def _ec_logs_append(camp_id: str, camp_name: str, verify: Any) -> None:
    sid = (EC_SHEETS_SPREADSHEET_ID or "").strip()
    if not sid:
        return
    try:
        logs = gd.read_sheet_withID(sid, "logs") or []
        logs.append(
            {
                "campId": camp_id,
                "campName": camp_name,
                "verify": verify,
                "date": datetime.now().strftime("%Y-%m-%d"),
                "response": "ok",
            }
        )
        gd.create_or_update_sheet_from_dicts_withId(sid, "logs", logs)
    except Exception as e:
        logger.warning("EC WL sync: logs append failed: %s", e)


def sync_ec_exploration_wl_from_keitaro_sales(
    *,
    lookback_days: Optional[int] = None,
    dry_run: bool = False,
    client: Optional[KeitaroClient] = None,
    reactivate_paused_campaigns: bool = True,
) -> Dict[str, Any]:
    """
    Ensure converting EC sources are on ``trackExploration.wl`` and campaign
    ``whitelistsources``. Reactivate blacklisted sources at absolute ``target_bid``.
    Append missing ``ECQualityWL`` rows.
    """
    sheet_id = (EC_SHEETS_SPREADSHEET_ID or "").strip()
    if not sheet_id:
        raise RuntimeError("EC_SHEETS_SPREADSHEET_ID is not configured")

    days = max(1, int(lookback_days or EC_EXPLORATION_WL_LOOKBACK_DAYS))
    target_bid = float(EC_EXPLORATION_WL_REACTIVATE_TARGET_BID_USD)

    gd.append_missing_headers_row1(sheet_id, TAB_EXPLORATION, HEADERS_EXPLORATION, create_if_missing=False)
    rows = gd.read_sheet_withID(sheet_id, TAB_EXPLORATION) or []
    idx = _exploration_index(rows)

    sales_by_key = collect_ec_sale_sources_detailed(lookback_days=days, client=client)

    campaigns_updated = 0
    sources_appended = 0
    sources_reactivated = 0
    sources_reactivate_failed = 0
    campaigns_activated = 0
    details: List[Dict[str, Any]] = []
    unmatched_keys: List[Dict[str, Any]] = []
    quality_wl_candidates: List[Tuple[str, str, str]] = []
    patches: Dict[str, Dict[str, str]] = {}

    for key, sale_pairs in sorted(sales_by_key.items()):
        row = idx.get(key)
        if not row:
            unmatched_keys.append(
                {"brand_key": key[0], "geo": key[1], "sale_sources": len(sale_pairs)}
            )
            continue

        wl = _parse_wl(row.get("wl"))
        if isinstance(row.get("wl"), list):
            wl = [str(x).strip() for x in row["wl"] if str(x).strip()]
        wl_set = set(wl)
        sale_subs_norm = [str(s).strip() for s, _ in sale_pairs if str(s).strip()]
        to_add = [s for s in sale_subs_norm if s not in wl_set]
        cid = str(row.get("campId") or "").strip()
        cname = str(row.get("campName") or "").strip()
        sub6_by_sub = {str(s).strip(): str(s6).strip() for s, s6 in sale_pairs if str(s).strip()}

        camp_data = _camp_payload(cid) if cid else None
        to_reactivate = _subs_needing_reactivation_ec(camp_data, sale_subs_norm, wl_set, to_add)

        if not to_add and not to_reactivate:
            # Still ensure QualityWL rows for converting sources already on WL.
            if cid:
                for sub in sale_subs_norm:
                    quality_wl_candidates.append(
                        (
                            cid,
                            sub,
                            _quality_wl_label_from_sub_id_6(
                                sub6_by_sub.get(sub, ""),
                                str(row.get("brand") or key[0]),
                                str(row.get("geo") or key[1]),
                                cname,
                            ),
                        )
                    )
            continue

        for sub in sale_subs_norm:
            if sub and cid:
                quality_wl_candidates.append(
                    (
                        cid,
                        sub,
                        _quality_wl_label_from_sub_id_6(
                            sub6_by_sub.get(sub, ""),
                            str(row.get("brand") or key[0]),
                            str(row.get("geo") or key[1]),
                            cname,
                        ),
                    )
                )

        reactivated_ok: List[str] = []
        reactivate_failed: List[str] = []
        activated = False

        if dry_run:
            reactivated_ok = list(to_reactivate)
            if (
                reactivate_paused_campaigns
                and camp_data
                and str(camp_data.get("status") or "").strip().lower() == "paused"
            ):
                activated = True
        else:
            if to_reactivate and cid:
                try:
                    result = reactivate_sources_ec(
                        cid,
                        to_reactivate,
                        target_bid=target_bid,
                        also_whitelist=True,
                    )
                    reactivated_ok = list(result.get("reactivated") or [])
                except Exception as e:
                    logger.warning("EC reactivate failed for %s: %s", cid, e)
                    reactivate_failed = list(to_reactivate)
            if to_add and cid and not to_reactivate:
                # Reactivate path already whitelists; otherwise push API WL for new-only adds.
                try:
                    whiteListSources(cid, to_add)
                except Exception as e:
                    logger.warning("EC whiteListSources failed for %s: %s", cid, e)
            if (
                reactivate_paused_campaigns
                and cid
                and camp_data
                and str(camp_data.get("status") or "").strip().lower() == "paused"
            ):
                try:
                    activate_campaignWitId(cid)
                    activated = True
                    row["status"] = "active"
                except Exception as e:
                    logger.warning("EC activate campaign %s failed: %s", cid, e)

        new_wl = wl + to_add if to_add else wl
        details.append(
            {
                "campaign_id": cid,
                "campaign_name": cname,
                "brand": key[0],
                "geo": key[1],
                "appended": to_add,
                "reactivated": reactivated_ok,
                "reactivate_failed": reactivate_failed,
                "campaign_activated": activated,
                "target_bid_usd": target_bid,
                "wl_before": len(wl),
                "wl_after": len(new_wl),
            }
        )
        campaigns_updated += 1
        sources_appended += len(to_add)
        sources_reactivated += len(reactivated_ok)
        sources_reactivate_failed += len(reactivate_failed)
        if activated:
            campaigns_activated += 1

        if dry_run:
            continue

        if to_add:
            row["wl"] = _format_wl(new_wl)
        parts: List[str] = []
        if to_add:
            parts.append(f"appended {len(to_add)} to WL")
        if reactivated_ok:
            parts.append(f"reactivated {len(reactivated_ok)} @ ${target_bid:.2f} cpcbysource")
        if reactivate_failed:
            parts.append(f"reactivate failed {len(reactivate_failed)}")
        if activated:
            parts.append("campaign set active")
        patch_key = cid or f"name:{cname.lower()}"
        patch: Dict[str, str] = {"wl": str(row.get("wl") or _format_wl(new_wl))}
        if "logs" in row or parts:
            # Prefer dedicated logs column when present; else EC sheet uses separate logs tab.
            if "logs" in row:
                row["logs"] = _append_logs_cell(row.get("logs", ""), "WL sync: " + "; ".join(parts))
                patch["logs"] = str(row.get("logs") or "")
            if "lastAction" in row:
                row["lastAction"] = "wl-from-sales"
                patch["lastAction"] = "wl-from-sales"
            if parts:
                _ec_logs_append(cid, cname, "WL sync: " + "; ".join(parts))
        if activated:
            patch["status"] = "active"
        patches[patch_key] = patch

    quality_wl_result = append_ec_quality_wl_rows(
        quality_wl_candidates, dry_run=dry_run, sheet_id=sheet_id
    )

    if patches and not dry_run:
        _write_exploration_wl_patches(sheet_id, patches)

    summary = {
        "dry_run": dry_run,
        "lookback_days": days,
        "target_bid_usd": target_bid,
        "sale_brand_geo_keys": len(sales_by_key),
        "exploration_rows": len(rows),
        "campaigns_updated": campaigns_updated,
        "sources_appended": sources_appended,
        "sources_reactivated": sources_reactivated,
        "sources_reactivate_failed": sources_reactivate_failed,
        "campaigns_activated": campaigns_activated,
        "unmatched_sale_keys": len(unmatched_keys),
        "details": details,
        "unmatched": unmatched_keys[:30],
        "quality_wl": quality_wl_result,
    }
    logger.info(
        "EC exploration WL sync (%s): campaigns=%s appended=%s reactivated=%s activated=%s quality_wl=%s",
        "dry-run" if dry_run else "apply",
        campaigns_updated,
        sources_appended,
        sources_reactivated,
        campaigns_activated,
        quality_wl_result.get("appended"),
    )
    return summary


def _sum_clicks(stats: Any) -> Dict[str, int]:
    """Map source -> clicks from adv-stats-by-source payload."""
    out: Dict[str, int] = {}
    rows = []
    if isinstance(stats, dict):
        rows = stats.get("stats") or []
    elif isinstance(stats, list):
        rows = stats
    for row in rows:
        if not isinstance(row, dict):
            continue
        src = str(row.get("source") or "").strip()
        if not src:
            continue
        try:
            clicks = int(float(row.get("clicks") or 0))
        except (TypeError, ValueError):
            clicks = 0
        out[src] = out.get(src, 0) + clicks
    return out


def refresh_ec_quality_wl(*, sheet_id: Optional[str] = None) -> Dict[str, Any]:
    """Even-hour style refresh: clicks windows + bid (cpcbysource) + ECstatus."""
    from datetime import date

    sid = ensure_ec_quality_wl_sheet(sheet_id)
    sheet = gd.read_sheet_withID(sid, TAB_QUALITY_WL) or []
    if not sheet:
        return {"rows": 0, "updated": 0}

    today = date.today()
    d_today = today.strftime("%Y-%m-%d")
    d_yest = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    d_7 = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    d_30 = (today - timedelta(days=30)).strftime("%Y-%m-%d")

    camp_cache: Dict[str, Optional[dict]] = {}
    stats_cache: Dict[Tuple[str, str, str], Dict[str, int]] = {}

    def _stats(cid: str, start: str, end: str) -> Dict[str, int]:
        key = (cid, start, end)
        if key in stats_cache:
            return stats_cache[key]
        try:
            raw = get_campaigns_statsBySource(cid, start, end)
            mapped = _sum_clicks(raw)
        except Exception as e:
            logger.warning("ECQualityWL stats %s %s..%s: %s", cid, start, end, e)
            mapped = {}
        stats_cache[key] = mapped
        return mapped

    updated = 0
    for row in sheet:
        cid = str(row.get("CampaignID") or "").strip()
        sub = str(row.get("SUBID") or "").strip()
        if not cid or not sub:
            row["ECstatus"] = "missing CampaignID/SUBID"
            row["lastUpdate"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            updated += 1
            continue
        if cid not in camp_cache:
            camp_cache[cid] = _camp_payload(cid)
        camp = camp_cache[cid]
        if camp:
            row["ECstatus"] = str(camp.get("status") or "")
            cpc = camp.get("cpcbysource") or {}
            if isinstance(cpc, dict) and sub in cpc:
                row["bid"] = str(cpc.get(sub))
            else:
                row["bid"] = str(camp.get("bid") or "")
        else:
            row["ECstatus"] = "campaign not found"
            row["bid"] = ""
        clicks30 = _stats(cid, d_30, d_today)
        clicks7 = _stats(cid, d_7, d_today)
        clicks_y = _stats(cid, d_yest, d_yest)
        clicks_t = _stats(cid, d_today, d_today)
        row["clicks30"] = str(clicks30.get(sub, 0))
        row["clicks7"] = str(clicks7.get(sub, 0))
        row["clicksYest"] = str(clicks_y.get(sub, 0))
        row["clicksToday"] = str(clicks_t.get(sub, 0))
        row["url"] = ""
        row["lastUpdate"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        updated += 1

    gd.create_or_update_sheet_from_dicts_withId(sid, TAB_QUALITY_WL, sheet)
    return {"rows": len(sheet), "updated": updated, "campaigns": len(camp_cache)}
