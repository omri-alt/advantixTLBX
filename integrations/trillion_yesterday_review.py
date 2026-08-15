"""
Yesterday domain-demand underdelivery review + Trillion budget/CPC recommendations.

Reads archived ``yesterday_summary_by_geo`` (or dated tab), compares demand vs delivered
clicks, checks whether each hub RON hit its daily budget yesterday (Trillion report API),
and recommends either a daily-budget bump or CPC bump. Selected rows can be applied in bulk
via ``update_ron``.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from config import (
    DOMAIN_DEMAND_SUMMARY_BY_GEO_TAB,
    KEYTR,
    TRILLION_YESTERDAY_BUDGET_BUMP_PER_1K_CLICKS,
    TRILLION_YESTERDAY_BUDGET_REACHED_PCT,
    TRILLION_YESTERDAY_CPC_BUMP,
)

logger = logging.getLogger(__name__)


def _yesterday_date_str() -> str:
    from integrations.blend_cap_progress import _today_in_report_tz

    today = datetime.strptime(_today_in_report_tz(), "%Y-%m-%d").date()
    return (today - timedelta(days=1)).isoformat()


def _clamp_budget(amount: float) -> float:
    return max(10.0, min(10000.0, round(amount, 2)))


def _clamp_bid(amount: float) -> float:
    """RON SubP bids may be as low as $0.005 (keyword campaigns use $0.05)."""
    return max(0.005, min(100.0, round(amount, 4)))


def _budget_reached_yesterday(cost: Optional[float], daily_limit: Optional[float]) -> bool:
    if cost is None or daily_limit is None or daily_limit < 10.0:
        return False
    threshold = daily_limit * (float(TRILLION_YESTERDAY_BUDGET_REACHED_PCT) / 100.0)
    return cost >= threshold


def _load_yesterday_segments() -> Tuple[List[Dict[str, Any]], str, List[str]]:
    from integrations.domain_demand import load_summary_by_geo_from_sheet

    yday = _yesterday_date_str()
    logs: List[str] = []
    tab = f"yesterday_{DOMAIN_DEMAND_SUMMARY_BY_GEO_TAB}"
    rows, tab_logs = load_summary_by_geo_from_sheet(tab=tab, date_str=yday)
    logs.extend(tab_logs)
    if rows:
        return rows, yday, logs

    dated_tab = f"{DOMAIN_DEMAND_SUMMARY_BY_GEO_TAB}_{yday}"
    rows2, tab_logs2 = load_summary_by_geo_from_sheet(tab=dated_tab, date_str=yday)
    logs.extend(tab_logs2)
    if rows2:
        return rows2, yday, logs

    logs.append(
        f"No yesterday archive tab found ({tab} or {dated_tab}); "
        "run nightly rollover or pick another date after daily workflow"
    )
    return [], yday, logs


def _index_list_campaigns(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        name = str(row.get("Campaign") or "").strip()
        if not name:
            continue
        out[name] = row
    return out


def _index_report_by_campaign(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        name = str(row.get("Campaign") or "").strip()
        if not name:
            continue
        out[name] = row
    return out


def _recommendation_for_row(
    *,
    missing_clicks: int,
    budget_reached: bool,
    current_daily_limit: Optional[float],
    current_my_bid: Optional[float],
) -> Dict[str, Any]:
    missing = max(0, int(missing_clicks))
    if budget_reached:
        delta = float(TRILLION_YESTERDAY_BUDGET_BUMP_PER_1K_CLICKS) * (missing / 1000.0)
        base = current_daily_limit if current_daily_limit is not None else 10.0
        new_limit = _clamp_budget(base + delta)
        return {
            "recommended_action": "increase_budget",
            "recommendation_label": f"Raise daily budget +${delta:.2f} → ${new_limit:.2f}",
            "delta_budget": round(delta, 2),
            "new_daily_limit": new_limit,
            "new_my_bid": None,
            "delta_cpc": None,
        }
    base_bid = current_my_bid if current_my_bid is not None else 0.005
    delta_cpc = float(TRILLION_YESTERDAY_CPC_BUMP)
    new_bid = _clamp_bid(base_bid + delta_cpc)
    return {
        "recommended_action": "increase_cpc",
        "recommendation_label": f"Raise CPC +{delta_cpc:.3f} → ${new_bid:.3f}",
        "delta_budget": None,
        "new_daily_limit": None,
        "new_my_bid": new_bid,
        "delta_cpc": delta_cpc,
    }


def build_yesterday_review_payload(
    *,
    date_str: Optional[str] = None,
) -> Dict[str, Any]:
    """Build review rows for underdelivered geo×device segments on ``date_str`` (default: yesterday)."""
    from integrations.domain_demand import build_trillion_segment_map
    from integrations.trillion import fetch_report, list_campaigns, parse_trillion_money

    logs: List[str] = []
    target_day = (date_str or _yesterday_date_str()).strip()

    if date_str:
        from integrations.domain_demand import load_summary_by_geo_from_sheet

        segments, load_logs = load_summary_by_geo_from_sheet(
            tab=f"yesterday_{DOMAIN_DEMAND_SUMMARY_BY_GEO_TAB}",
            date_str=target_day,
        )
        if not segments:
            segments, more_logs = load_summary_by_geo_from_sheet(
                tab=f"{DOMAIN_DEMAND_SUMMARY_BY_GEO_TAB}_{target_day}",
                date_str=target_day,
            )
            load_logs.extend(more_logs)
        logs.extend(load_logs)
    else:
        segments, inferred_day, load_logs = _load_yesterday_segments()
        logs.extend(load_logs)
        if inferred_day:
            target_day = inferred_day

    if not (KEYTR or "").strip():
        return {
            "status": "error",
            "error": "KEYTR not configured",
            "date": target_day,
            "rows": [],
            "logs": logs,
        }

    tr_map, tr_logs = build_trillion_segment_map()
    logs.extend(tr_logs)

    try:
        list_rows = list_campaigns(KEYTR)
        list_by_name = _index_list_campaigns(list_rows)
    except Exception as e:
        return {
            "status": "error",
            "error": f"list_campaigns failed: {e}",
            "date": target_day,
            "rows": [],
            "logs": logs,
        }

    report_rows: List[Dict[str, Any]] = []
    report_err: Optional[str] = None
    try:
        if target_day == _yesterday_date_str():
            report_rows = fetch_report(KEYTR, period="yesterday", groupby="keyword")
        else:
            report_rows = fetch_report(
                KEYTR,
                from_date=target_day,
                to_date=target_day,
                groupby="day_keyword",
            )
        if report_rows and len(report_rows) == 1 and report_rows[0].get("Status") == "Processing":
            report_err = "Trillion report still processing — retry in a minute"
            report_rows = []
    except Exception as e:
        report_err = str(e)
        logs.append(f"Trillion report for {target_day}: {e}")

    report_by_name = _index_report_by_campaign(report_rows)
    hub_rons = {str(v.get("campaign") or "") for v in tr_map.values() if v.get("campaign")}

    review_rows: List[Dict[str, Any]] = []
    underdelivered_count = 0
    budget_reached_count = 0

    for seg in segments:
        demand = int(seg.get("demand_clicks") or 0)
        delivered = int(seg.get("delivered_clicks") or 0)
        missing = int(seg.get("remaining") or max(0, demand - delivered))
        if demand <= 0 or missing <= 0:
            continue

        geo = str(seg.get("geo") or "").lower()
        device = str(seg.get("device") or "").lower()
        tr = tr_map.get((geo, device)) or {}
        ron = str(seg.get("trillion_campaign") or tr.get("campaign") or "").strip()
        if not ron:
            review_rows.append(
                {
                    "key": f"{geo}/{device}",
                    "geo": geo,
                    "device": device,
                    "demand_clicks": demand,
                    "delivered_clicks": delivered,
                    "missing_clicks": missing,
                    "fill_pct": seg.get("fill_pct"),
                    "trillion_campaign": "",
                    "budget_reached": False,
                    "recommended_action": None,
                    "recommendation_label": "No mapped Trillion RON",
                    "applicable": False,
                }
            )
            continue

        list_row = list_by_name.get(ron) or {}
        report_row = report_by_name.get(ron) or {}

        current_limit = parse_trillion_money(list_row.get("Daily_Limit"))
        if current_limit is None:
            current_limit = parse_trillion_money(report_row.get("Daily_Limit"))
        current_bid = parse_trillion_money(list_row.get("My_Bid"))
        if current_bid is None:
            current_bid = parse_trillion_money(report_row.get("My_Bid"))

        yesterday_cost = parse_trillion_money(report_row.get("Cost"))
        yesterday_traffic = None
        traffic_raw = report_row.get("Traffic")
        if traffic_raw is not None and str(traffic_raw).strip():
            try:
                yesterday_traffic = int(float(str(traffic_raw).strip()))
            except (TypeError, ValueError):
                yesterday_traffic = None

        budget_reached = _budget_reached_yesterday(yesterday_cost, current_limit)
        if budget_reached:
            budget_reached_count += 1

        rec = _recommendation_for_row(
            missing_clicks=missing,
            budget_reached=budget_reached,
            current_daily_limit=current_limit,
            current_my_bid=current_bid,
        )
        underdelivered_count += 1
        review_rows.append(
            {
                "key": f"{geo}/{device}",
                "geo": geo,
                "device": device,
                "demand_clicks": demand,
                "delivered_clicks": delivered,
                "missing_clicks": missing,
                "fill_pct": seg.get("fill_pct"),
                "trillion_campaign": ron,
                "trillion_status": str(list_row.get("Status") or seg.get("trillion_status") or ""),
                "daily_limit": current_limit,
                "my_bid": current_bid,
                "yesterday_cost": yesterday_cost,
                "yesterday_traffic": yesterday_traffic,
                "budget_reached": budget_reached,
                "recommended_action": rec["recommended_action"],
                "recommendation_label": rec["recommendation_label"],
                "delta_budget": rec["delta_budget"],
                "new_daily_limit": rec["new_daily_limit"],
                "delta_cpc": rec["delta_cpc"],
                "new_my_bid": rec["new_my_bid"],
                "applicable": True,
            }
        )

    review_rows.sort(key=lambda r: (-int(r.get("missing_clicks") or 0), r.get("geo") or "", r.get("device") or ""))

    status = "ok"
    error: Optional[str] = None
    if not segments:
        status = "empty"
        error = f"No archived domain-demand rows for {target_day}"
    elif underdelivered_count == 0:
        status = "ok"
        logs.append("No underdelivered segments yesterday (all demand met or zero demand)")

    return {
        "status": status,
        "error": error,
        "date": target_day,
        "rows": review_rows,
        "summary": {
            "segments_loaded": len(segments),
            "underdelivered": underdelivered_count,
            "budget_reached": budget_reached_count,
            "hub_ron_count": len(hub_rons),
            "report_rows": len(report_rows),
        },
        "report_error": report_err,
        "formula": {
            "budget_bump_per_1k_clicks": TRILLION_YESTERDAY_BUDGET_BUMP_PER_1K_CLICKS,
            "cpc_bump": TRILLION_YESTERDAY_CPC_BUMP,
            "budget_reached_pct": TRILLION_YESTERDAY_BUDGET_REACHED_PCT,
        },
        "logs": logs,
    }


def apply_yesterday_review_actions(
    keys: List[str],
    *,
    dry_run: bool = False,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Apply Trillion ``update_ron`` for selected review row keys (``geo/device``)."""
    from integrations.trillion import TrillionClientError, update_ron

    if not (KEYTR or "").strip():
        return {"ok": False, "error": "KEYTR not configured", "actions": []}

    data = payload or build_yesterday_review_payload()
    rows_by_key = {str(r.get("key") or ""): r for r in (data.get("rows") or [])}
    wanted = [str(k).strip() for k in keys if str(k).strip()]
    actions: List[Dict[str, Any]] = []
    applied = 0
    errors: List[str] = []

    for key in wanted:
        row = rows_by_key.get(key)
        action: Dict[str, Any] = {"key": key, "status": "skipped"}
        if not row:
            action["status"] = "not_found"
            actions.append(action)
            continue
        if not row.get("applicable"):
            action["status"] = "not_applicable"
            action["reason"] = row.get("recommendation_label")
            actions.append(action)
            continue
        ron = str(row.get("trillion_campaign") or "").strip()
        rec_type = str(row.get("recommended_action") or "")
        action.update(
            {
                "ron": ron,
                "recommended_action": rec_type,
                "geo": row.get("geo"),
                "device": row.get("device"),
            }
        )
        if not ron:
            action["status"] = "unmapped"
            actions.append(action)
            continue
        if dry_run:
            action["status"] = "would_apply"
            action["new_daily_limit"] = row.get("new_daily_limit")
            action["new_my_bid"] = row.get("new_my_bid")
            applied += 1
            actions.append(action)
            continue
        try:
            if rec_type == "increase_budget" and row.get("new_daily_limit") is not None:
                update_ron(KEYTR, ron=ron, daily_limit=float(row["new_daily_limit"]))
                action["status"] = "applied_budget"
                action["new_daily_limit"] = row.get("new_daily_limit")
            elif rec_type == "increase_cpc" and row.get("new_my_bid") is not None:
                update_ron(KEYTR, ron=ron, my_bid=float(row["new_my_bid"]))
                action["status"] = "applied_cpc"
                action["new_my_bid"] = row.get("new_my_bid")
            else:
                action["status"] = "no_action"
            applied += 1
        except TrillionClientError as e:
            action["status"] = "error"
            action["error"] = str(e)
            errors.append(f"{key} {ron}: {e}")
        actions.append(action)

    return {
        "ok": not errors,
        "dry_run": dry_run,
        "requested": len(wanted),
        "applied": applied,
        "errors": errors,
        "actions": actions,
    }
