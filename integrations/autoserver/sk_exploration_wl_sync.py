"""

Append SK publisher subIds to ``SKtrackExploration.wl`` from Keitaro sale conversions.



Keitaro ``conversions/log`` rows with ``sub_id_6`` ending in ``-SK`` carry:

  - ``sub_id_6`` — ``{brand}-{GEO}-{prefix}-SK`` (affiliation / brand tag)

  - ``sub_id_5`` — SK publisher ``subId`` (same values as the sheet ``wl`` JSON list)



Statuses scanned: ``SaleOur``, ``LateSale`` (default lookback 30 days).

New sources are appended at the **end** of the existing ``wl`` list (order preserved).

Blacklisted sources (``bidFactor`` 0) are reactivated on the SK campaign via bulk bid-factor API.

Part 2: append new ``QualityWL`` rows (``CampaignID`` × ``SUBID``) at the end when missing.

"""

from __future__ import annotations



import json

import logging

import re

from collections import defaultdict

from datetime import date, datetime, timedelta, timezone

from pathlib import Path

from typing import Any, Dict, List, Optional, Set, Tuple



from config import (

    SK_EXPLORATION_WL_LOOKBACK_DAYS,

    SK_EXPLORATION_WL_REACTIVATE_TARGET_BID_USD,

    SK_OPTIMIZER_SHEET_ID,

)

from integrations.autoserver import gdocs_as as gd

from integrations.autoserver.sk_optimizer import (

    HEADERS_EXPLORATION,

    TAB_EXPLORATION,

    _append_logs_cell,

    _format_wl,

    _parse_wl,

    _reactivate_sources_sk,

    _sk_tools_workbook_log,

    _stats_items_by_subid_today,

    resolve_wl_reactivate_bid_factor,

)

from integrations.keitaro import KeitaroClient, KeitaroClientError

from integrations.keitaro_conversions import iter_conversion_log



logger = logging.getLogger(__name__)



_SK_SUBID6_RE = re.compile(r"^(.+)-([A-Z]{2})-.+-SK$", re.IGNORECASE)

_SK_CAMPAIGN_PREFIX_RE = re.compile(r"(KLFIX|KLFLEX|KLTESTED|KLWL\d*)", re.IGNORECASE)

TAB_QUALITY_WL = "QualityWL"

_SALE_STATUSES = ("SaleOur", "LateSale")

_SK_LOG_COLUMNS = ("sub_id_5", "sub_id_6", "status", "datetime")

_BID_ZERO_EPS = 0.001

_BID_TARGET_EPS = 0.002



_STATE_PATH = Path(__file__).resolve().parents[2] / "data" / "sk_exploration_wl_sync_state.json"





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





def _quality_wl_label(brand: str, geo: str, campaign_name: str) -> str:

    """Sheet column A label, e.g. ``petitbeguin-FR-KLFLEX``."""

    brand_key = _norm_brand_key(brand)

    if not brand_key and campaign_name:

        brand_key = _norm_brand_key(campaign_name.split("-", 1)[0])

    geo_up = _quality_wl_geo_label(geo)

    prefix = "KLFLEX"

    m = _SK_CAMPAIGN_PREFIX_RE.search(campaign_name or "")

    if m:

        prefix = m.group(1).upper()

    return f"{brand_key}-{geo_up}-{prefix}"





def _blank_quality_wl_row(label: str, campaign_id: str, sub_id: str) -> Dict[str, str]:

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    cid = str(campaign_id).strip()

    sub = str(sub_id).strip()

    return {

        "": label,

        "CampaignID": cid,

        "SUBID": sub,

        "SKstatus": "",

        "winrate30": "",

        "winrate7": "",

        "winrateYest": "",

        "winrateToday": "",

        "bid": "",

        "url": f"https://app.sourceknowledge.com/agency/campaigns/{cid}/by-channel",

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





def append_quality_wl_rows(

    candidates: List[Tuple[str, str, str]],

    *,

    dry_run: bool = False,

) -> Dict[str, Any]:

    """

    Append ``QualityWL`` rows for ``(campaign_id, sub_id, label)`` at the end.

    Skips pairs that already exist on the tab.

    """

    if not candidates:

        return {

            "dry_run": dry_run,

            "appended": 0,

            "skipped_existing": 0,

            "quality_wl_rows_before": 0,

            "quality_wl_rows_after": 0,

            "details": [],

        }



    sheet = gd.read_sheet(TAB_QUALITY_WL)

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

        headers = list(sheet[0].keys()) if sheet else list(appended_rows[0].keys())

        merged: List[Dict[str, str]] = []

        for row in sheet:

            merged.append({h: "" if row.get(h) is None else str(row.get(h, "")) for h in headers})

        for row in appended_rows:

            merged.append({h: "" if row.get(h) is None else str(row.get(h, "")) for h in headers})

        gd.create_or_update_sheet_from_dicts(TAB_QUALITY_WL, merged)



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





def parse_sk_sub_id_6(sub_id_6: str) -> Optional[Tuple[str, str]]:

    """Return ``(brand_key, geo)`` from ``brand-GEO-prefix-SK`` or ``None``."""

    m = _SK_SUBID6_RE.match((sub_id_6 or "").strip())

    if not m:

        return None

    return _norm_brand_key(m.group(1)), _norm_geo(m.group(2))





def _exploration_index(rows: List[Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:

    """``(norm_brand, geo)`` -> first matching exploration row."""

    out: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for row in rows:

        brand = _norm_brand_key(str(row.get("brand") or ""))

        geo = _norm_geo(str(row.get("geo") or ""))

        if not brand or not geo:

            continue

        key = (brand, geo)

        if key not in out:

            out[key] = row

    return out





def _exploration_row_by_campaign_id(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:

    out: Dict[str, Dict[str, Any]] = {}

    for row in rows:

        cid = str(row.get("campaignId") or row.get("campId") or "").strip()

        if cid:

            out[cid] = row

    return out





def _append_subs_to_exploration_wl_row(row: Dict[str, Any], subs: List[str]) -> List[str]:

    """Append missing subs to a row's ``wl`` list; returns subs actually added."""

    wl = _parse_wl(row.get("wl"))

    wl_set = set(wl)

    added: List[str] = []

    for raw in subs:

        sub = str(raw or "").strip()

        if not sub or sub in wl_set:

            continue

        wl_set.add(sub)

        added.append(sub)

    if added:

        row["wl"] = _format_wl(wl + added)

    return added





def _backfill_exploration_wl_from_quality_appends(

    rows: List[Dict[str, Any]],

    quality_wl_result: Dict[str, Any],

    *,

    dry_run: bool = False,

) -> Dict[str, Any]:

    """

    Ensure ``SKtrackExploration.wl`` includes subs newly appended to QualityWL.

    Fixes cases where QualityWL rows were added but exploration ``wl`` was not updated.

    """

    appended = quality_wl_result.get("details") or []

    if not appended or dry_run:

        return {"sources_appended": 0, "rows_updated": 0}



    by_cid = _exploration_row_by_campaign_id(rows)

    sources_appended = 0

    rows_updated = 0

    for item in appended:

        cid = str(item.get("campaign_id") or "").strip()

        sub = str(item.get("sub_id") or "").strip()

        if not cid or not sub:

            continue

        row = by_cid.get(cid)

        if not row:

            continue

        added = _append_subs_to_exploration_wl_row(row, [sub])

        if added:

            sources_appended += len(added)

            rows_updated += 1

            row["lastAction"] = "wl-from-sales"

            row["logs"] = _append_logs_cell(

                row.get("logs", ""),

                "WL sync backfill: appended " + ", ".join(added) + " (QualityWL)",

            )

    return {"sources_appended": sources_appended, "rows_updated": rows_updated}





def collect_sk_sale_sources_by_brand_geo(

    *,

    lookback_days: Optional[int] = None,

    client: Optional[KeitaroClient] = None,

) -> Dict[Tuple[str, str], List[str]]:

    """

    Map ``(norm_brand, geo)`` -> ordered unique ``sub_id_5`` values (first-seen order).



    Scans Keitaro ``conversions/log`` for ``SaleOur`` and ``LateSale``.

    """

    days = max(1, int(lookback_days or SK_EXPLORATION_WL_LOOKBACK_DAYS))

    c = client or KeitaroClient()

    today = datetime.now(timezone.utc).date()

    start = today - timedelta(days=days)



    order: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(dict)

    seq = 0



    for status in _SALE_STATUSES:

        try:

            for row in iter_conversion_log(

                c,

                date_from=start,

                date_to=today,

                status=status,

                columns=list(_SK_LOG_COLUMNS),

            ):

                sub5 = str(row.get("sub_id_5") or "").strip()

                parsed = parse_sk_sub_id_6(str(row.get("sub_id_6") or ""))

                if not sub5 or not parsed:

                    continue

                key = parsed

                if sub5 not in order[key]:

                    order[key][sub5] = seq

                    seq += 1

        except KeitaroClientError as e:

            logger.warning("SK WL sync: Keitaro conversions/log status=%s failed: %s", status, e)



    out: Dict[Tuple[str, str], List[str]] = {}

    for key, subs in order.items():

        out[key] = [s for s, _ in sorted(subs.items(), key=lambda kv: kv[1])]

    return out





def _subs_needing_reactivation(

    campaign_id: int,

    sale_subs: List[str],

    wl_set: Set[str],

    to_add: List[str],

    *,

    stats_from: str,

    stats_to: str,

    campaign_cpc: Optional[float] = None,

    target_effective_bid_usd: Optional[float] = None,

) -> List[str]:

    """New WL subs + WL subs blacklisted or not at the target effective bid."""

    need: Set[str] = set(to_add)

    if not campaign_id or not sale_subs:

        return sorted(need)



    target_bid = (

        float(target_effective_bid_usd)

        if target_effective_bid_usd is not None

        else SK_EXPLORATION_WL_REACTIVATE_TARGET_BID_USD

    )



    per_sub, err = _stats_items_by_subid_today(campaign_id, stats_from, stats_to)

    if err:

        logger.warning("SK WL sync: stats for reactivate check %s: %s", campaign_id, err)

        return sorted(need)



    for sid in sale_subs:

        if sid in need:

            continue

        if sid not in wl_set:

            continue

        bf = per_sub.get(sid, {}).get("bidFactor")

        if bf is None:

            need.add(sid)

            continue

        bf_f = float(bf)

        if bf_f <= _BID_ZERO_EPS:

            need.add(sid)

            continue

        if campaign_cpc and campaign_cpc > 0:

            effective = bf_f * campaign_cpc

            if abs(effective - target_bid) > _BID_TARGET_EPS:

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

        logger.warning("SK exploration WL sync state read failed: %s", e)

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





def sync_exploration_wl_from_keitaro_sales(

    *,

    lookback_days: Optional[int] = None,

    dry_run: bool = False,

    client: Optional[KeitaroClient] = None,

) -> Dict[str, Any]:

    """

    Ensure converting SK sources are on ``SKtrackExploration.wl`` (append at end) and

    reactivated on the SK campaign (``bidFactor = target_bid_usd / campaign_cpc``, default $0.10).

    Also appends missing ``QualityWL`` rows (``CampaignID`` × ``SUBID``) at the end of that tab.

    """

    sheet_id = (SK_OPTIMIZER_SHEET_ID or "").strip()

    if not sheet_id:

        raise RuntimeError("SK_OPTIMIZER_SHEET_ID is not configured")



    days = max(1, int(lookback_days or SK_EXPLORATION_WL_LOOKBACK_DAYS))

    today = datetime.now(timezone.utc).date()

    stats_from = (today - timedelta(days=days)).strftime("%Y-%m-%d")

    stats_to = today.strftime("%Y-%m-%d")



    gd.append_missing_headers_row1(sheet_id, TAB_EXPLORATION, HEADERS_EXPLORATION)

    rows = gd.read_sheet_withID(sheet_id, TAB_EXPLORATION)

    idx = _exploration_index(rows)



    sales_by_key = collect_sk_sale_sources_by_brand_geo(

        lookback_days=days,

        client=client,

    )



    changed = False

    campaigns_updated = 0

    sources_appended = 0

    sources_reactivated = 0

    sources_reactivate_failed = 0

    details: List[Dict[str, Any]] = []

    unmatched_keys: List[Dict[str, Any]] = []

    quality_wl_candidates: List[Tuple[str, str, str]] = []



    for key, sale_subs in sorted(sales_by_key.items()):

        row = idx.get(key)

        if not row:

            unmatched_keys.append(

                {

                    "brand_key": key[0],

                    "geo": key[1],

                    "sale_sources": len(sale_subs),

                }

            )

            continue



        wl = _parse_wl(row.get("wl"))

        wl_set = set(wl)

        sale_subs_norm = [str(s).strip() for s in sale_subs if str(s).strip()]

        to_add = [s for s in sale_subs_norm if s not in wl_set]

        cid_raw = str(row.get("campaignId") or row.get("campId") or "").strip()

        cname = str(row.get("campaignName") or row.get("campName") or "").strip()

        wl_label = _quality_wl_label(str(row.get("brand") or ""), str(row.get("geo") or ""), cname)

        for sub in sale_subs:

            if sub and cid_raw:

                quality_wl_candidates.append((cid_raw, sub, wl_label))

        cid_int = 0

        try:

            cid_int = int(cid_raw)

        except (TypeError, ValueError):

            cid_int = 0



        campaign_cpc: Optional[float] = None

        reactivate_bid_factor: Optional[float] = None

        if cid_int:

            reactivate_bid_factor, campaign_cpc = resolve_wl_reactivate_bid_factor(cid_int)



        to_reactivate = _subs_needing_reactivation(

            cid_int,

            sale_subs_norm,

            wl_set,

            to_add,

            stats_from=stats_from,

            stats_to=stats_to,

            campaign_cpc=campaign_cpc,

        )



        if not to_add and not to_reactivate:

            continue



        reactivated_ok: List[str] = []

        reactivate_failed: List[str] = []



        if to_reactivate and cid_int and reactivate_bid_factor is not None:

            if dry_run:

                reactivated_ok = list(to_reactivate)

            else:

                reactivated_ok, reactivate_failed, _ = _reactivate_sources_sk(

                    cid_int,

                    to_reactivate,

                    bid_factor=reactivate_bid_factor,

                )

        elif to_reactivate and cid_int:

            reactivate_failed = list(to_reactivate)



        new_wl = wl + to_add if to_add else wl



        details.append(

            {

                "campaign_id": cid_raw,

                "campaign_name": cname,

                "brand": row.get("brand"),

                "geo": row.get("geo"),

                "appended": to_add,

                "reactivated": reactivated_ok,

                "reactivate_failed": reactivate_failed,

                "target_effective_bid_usd": SK_EXPLORATION_WL_REACTIVATE_TARGET_BID_USD,

                "campaign_cpc": campaign_cpc,

                "reactivate_bid_factor": reactivate_bid_factor,

                "wl_before": len(wl),

                "wl_after": len(new_wl),

            }

        )



        campaigns_updated += 1

        sources_appended += len(to_add)

        sources_reactivated += len(reactivated_ok)

        sources_reactivate_failed += len(reactivate_failed)



        if dry_run:

            continue



        if to_add:

            row["wl"] = _format_wl(new_wl)

            changed = True

        parts: List[str] = []

        if to_add:

            parts.append(f"appended {len(to_add)} to WL")

        if reactivated_ok:

            bid_note = (

                f"${SK_EXPLORATION_WL_REACTIVATE_TARGET_BID_USD:.2f}"

                f" (bf={reactivate_bid_factor:.4f}, cpc={campaign_cpc})"

                if reactivate_bid_factor is not None and campaign_cpc is not None

                else f"${SK_EXPLORATION_WL_REACTIVATE_TARGET_BID_USD:.2f}"

            )

            parts.append(f"reactivated {len(reactivated_ok)} @ {bid_note}")

        if reactivate_failed:

            parts.append(f"reactivate failed {len(reactivate_failed)}")

        if parts:

            row["lastAction"] = "wl-from-sales"

            row["logs"] = _append_logs_cell(row.get("logs", ""), "WL sync: " + "; ".join(parts))

            changed = True

            _sk_tools_workbook_log(

                cid_raw,

                cname,

                "SK exploration WL sync",

                {

                    "appended": to_add,

                    "reactivated": reactivated_ok,

                    "reactivate_failed": reactivate_failed,

                    "lookback_days": days,

                },

            )



    quality_wl_result = append_quality_wl_rows(quality_wl_candidates, dry_run=dry_run)



    backfill = _backfill_exploration_wl_from_quality_appends(rows, quality_wl_result, dry_run=dry_run)

    if int(backfill.get("sources_appended") or 0) > 0:

        sources_appended += int(backfill["sources_appended"])

        changed = True



    if changed and not dry_run:

        gd.create_or_update_sheet_from_dicts_withId(sheet_id, TAB_EXPLORATION, rows)



    summary = {

        "dry_run": dry_run,

        "lookback_days": days,

        "target_effective_bid_usd": SK_EXPLORATION_WL_REACTIVATE_TARGET_BID_USD,

        "sale_brand_geo_keys": len(sales_by_key),

        "exploration_rows": len(rows),

        "campaigns_updated": campaigns_updated,

        "sources_appended": sources_appended,

        "sources_reactivated": sources_reactivated,

        "sources_reactivate_failed": sources_reactivate_failed,

        "unmatched_sale_keys": len(unmatched_keys),

        "details": details,

        "unmatched": unmatched_keys[:30],

        "quality_wl": quality_wl_result,

        "exploration_wl_backfill": backfill,

    }

    logger.info(

        "SK exploration WL sync (%s): campaigns=%s appended=%s reactivated=%s reactivate_failed=%s quality_wl=%s",

        "dry-run" if dry_run else "apply",

        campaigns_updated,

        sources_appended,

        sources_reactivated,

        sources_reactivate_failed,

        quality_wl_result.get("appended"),

    )

    if not dry_run:

        from integrations.sk_exploration_wl_sync_run_history import record_last_run

        record_last_run(summary)

    return summary


def normalize_exploration_wl_format(*, dry_run: bool = False) -> Dict[str, Any]:
    """Rewrite ``SKtrackExploration.wl`` cells to single-quote list format."""
    sheet_id = (SK_OPTIMIZER_SHEET_ID or "").strip()
    if not sheet_id:
        raise RuntimeError("SK_OPTIMIZER_SHEET_ID is not configured")

    gd.append_missing_headers_row1(sheet_id, TAB_EXPLORATION, HEADERS_EXPLORATION)
    rows = gd.read_sheet_withID(sheet_id, TAB_EXPLORATION)
    fixed = 0
    changed = False
    for row in rows:
        raw = str(row.get("wl") or "").strip()
        if not raw:
            continue
        parsed = _parse_wl(raw)
        formatted = _format_wl(parsed)
        if raw != formatted:
            fixed += 1
            if not dry_run:
                row["wl"] = formatted
                changed = True

    if changed and not dry_run:
        gd.create_or_update_sheet_from_dicts_withId(sheet_id, TAB_EXPLORATION, rows)

    return {"dry_run": dry_run, "rows_fixed": fixed, "exploration_rows": len(rows)}


