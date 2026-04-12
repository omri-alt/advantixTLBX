"""
Dashboard + detail summaries for daily conversion postbacks UI.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from config import KELKOO_RAW_REPORT_GEOS
from integrations.daily_conversion_postback_state import load_state
from integrations.daily_conversion_postbacks import default_report_date_str
from integrations.daily_postbacks_run_history import load_last_runs

_DATE_KEY = re.compile(r"^\d{4}-\d{2}-\d{2}$")

FEED_META: Tuple[Tuple[str, str], ...] = (
    ("kelkoo1", "Kelkoo feed 1"),
    ("kelkoo2", "Kelkoo feed 2"),
    ("adexa", "Adexa (feed 4)"),
    ("yadore", "Yadore (feed 3)"),
)


def _geo_ts_max(st: Dict[str, Any]) -> Optional[str]:
    best: Optional[str] = None
    for k in ("completed_at_utc", "last_updated_utc", "fetch_at_utc"):
        v = st.get(k)
        if isinstance(v, str) and v and (best is None or v > best):
            best = v
    return best


def _pick_primary_date(src: Dict[str, Any]) -> Optional[str]:
    keys = [k for k in src if isinstance(k, str) and _DATE_KEY.match(k)]
    return max(keys) if keys else None


def kelkoo_state_detail(
    data: Dict[str, Any],
    feed_key: str,
    report_date: str,
    geo_order: Sequence[str],
) -> Dict[str, Any]:
    bucket = ((data.get("sources") or {}).get(feed_key) or {}).get(report_date) or {}
    geos = bucket.get("geos") if isinstance(bucket.get("geos"), dict) else {}
    rows: List[Dict[str, Any]] = []
    done: List[str] = []
    partial: List[str] = []
    err: List[str] = []
    not_started: List[str] = []
    max_ts: Optional[str] = None

    for g in geo_order:
        st = geos.get(g)
        if not isinstance(st, dict):
            not_started.append(g)
            continue
        tmax = _geo_ts_max(st)
        if tmax and (max_ts is None or tmax > max_ts):
            max_ts = tmax
        status = str(st.get("status") or "").lower()
        row = {
            "geo": g,
            "status": status or "—",
            "rows_in_file": st.get("rows_in_file"),
            "postbacks_sent": st.get("postbacks_sent"),
            "next_row_index": st.get("next_row_index"),
            "row_stage": st.get("row_stage"),
            "fetch_ok": st.get("fetch_ok"),
            "completed_at_utc": st.get("completed_at_utc"),
            "last_error": (str(st.get("last_error") or "")[:200] or None),
        }
        rows.append(row)
        if status == "done":
            done.append(g)
        elif status == "error":
            err.append(g)
        else:
            partial.append(g)

    return {
        "report_date": report_date,
        "rows": rows,
        "done_geos": sorted(done),
        "partial_geos": sorted(partial),
        "error_geos": sorted(err),
        "not_started_geos": not_started,
        "done_count": len(done),
        "total_geos": len(geo_order),
        "max_state_ts": max_ts,
    }


def flat_state_detail(data: Dict[str, Any], feed_key: str, report_date: str) -> Dict[str, Any]:
    bucket = ((data.get("sources") or {}).get(feed_key) or {}).get(report_date) or {}
    fl = bucket.get("flat") if isinstance(bucket.get("flat"), dict) else {}
    max_ts: Optional[str] = None
    for k in ("completed_at_utc", "last_updated_utc", "fetch_at_utc"):
        v = fl.get(k)
        if isinstance(v, str) and v and (max_ts is None or v > max_ts):
            max_ts = v
    return {
        "report_date": report_date,
        "flat": fl,
        "max_state_ts": max_ts,
    }


def resolve_report_date_for_feed(data: Dict[str, Any], feed_key: str, prefer: Optional[str]) -> str:
    src = (data.get("sources") or {}).get(feed_key) or {}
    if prefer and _DATE_KEY.match(prefer or "") and prefer in src:
        return prefer
    primary = _pick_primary_date(src)
    if primary:
        return primary
    return prefer or ""


def build_dashboard_cards(state_path: Path) -> List[Dict[str, Any]]:
    data = load_state(state_path)
    history = load_last_runs()
    geo_order = list(KELKOO_RAW_REPORT_GEOS) if KELKOO_RAW_REPORT_GEOS else []

    cards: List[Dict[str, Any]] = []
    for key, title in FEED_META:
        src = (data.get("sources") or {}).get(key) or {}
        primary = _pick_primary_date(src)
        hist = history.get(key) if isinstance(history.get(key), dict) else {}

        last_at = hist.get("at_utc") if isinstance(hist.get("at_utc"), str) else None
        state_ts: Optional[str] = None
        kelkoo_extra: Dict[str, Any] = {}
        flat_extra: Dict[str, Any] = {}

        if key in ("kelkoo1", "kelkoo2") and geo_order:
            if primary:
                det = kelkoo_state_detail(data, key, primary, geo_order)
                state_ts = det.get("max_state_ts")
                kelkoo_extra = {
                    "primary_date": primary,
                    "done_count": det["done_count"],
                    "total_geos": det["total_geos"],
                    "done_sample": det["done_geos"][:8],
                    "has_more_done": len(det["done_geos"]) > 8,
                    "error_count": len(det["error_geos"]),
                    "partial_count": len(det["partial_geos"]),
                    "not_started_count": len(det["not_started_geos"]),
                }
            else:
                kelkoo_extra = {
                    "primary_date": None,
                    "done_count": 0,
                    "total_geos": len(geo_order),
                    "done_sample": [],
                    "has_more_done": False,
                    "error_count": 0,
                    "partial_count": 0,
                    "not_started_count": len(geo_order),
                }
        elif key in ("adexa", "yadore"):
            if primary:
                fd = flat_state_detail(data, key, primary)
                state_ts = fd.get("max_state_ts")
                fl = fd.get("flat") or {}
                flat_extra = {
                    "primary_date": primary,
                    "run_status": str(fl.get("status") or "—"),
                    "postbacks_sent": fl.get("postbacks_sent"),
                    "next_index": fl.get("next_index"),
                    "total_items": fl.get("total_items"),
                }
            else:
                flat_extra = {
                    "primary_date": None,
                    "run_status": "—",
                    "postbacks_sent": None,
                    "next_index": None,
                    "total_items": None,
                }

        display_ts = last_at or state_ts

        cards.append(
            {
                "key": key,
                "title": title,
                "last_run_at": display_ts,
                "last_run_from_history": bool(last_at),
                "history": hist,
                "primary_date": primary,
                "kelkoo": kelkoo_extra,
                "flat": flat_extra,
            }
        )
    return cards


def feed_detail_context(
    state_path: Path,
    feed_key: str,
    report_date_query: Optional[str],
) -> Dict[str, Any]:
    data = load_state(state_path)
    history = load_last_runs().get(feed_key)
    if not isinstance(history, dict):
        history = {}

    geo_order = list(KELKOO_RAW_REPORT_GEOS) if KELKOO_RAW_REPORT_GEOS else []
    rd = resolve_report_date_for_feed(data, feed_key, (report_date_query or "").strip() or None)
    if not rd:
        rd = default_report_date_str()

    kelkoo_detail: Optional[Dict[str, Any]] = None
    flat_detail: Optional[Dict[str, Any]] = None
    if feed_key in ("kelkoo1", "kelkoo2") and rd and geo_order:
        kelkoo_detail = kelkoo_state_detail(data, feed_key, rd, geo_order)
    elif feed_key in ("adexa", "yadore") and rd:
        flat_detail = flat_state_detail(data, feed_key, rd)

    title = dict(FEED_META).get(feed_key, feed_key)
    return {
        "feed_key": feed_key,
        "title": title,
        "history": history,
        "report_date": rd,
        "kelkoo_detail": kelkoo_detail,
        "flat_detail": flat_detail,
    }
