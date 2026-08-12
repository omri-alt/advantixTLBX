"""
API/cache helper for homepage Domain demand (hub campaign 94) fill widget.

Live payload comes from ``build_domain_demand_payload(rebuild_demand=False)`` so demand
lines stay as written by the morning bill, while delivered clicks refresh from Keitaro.
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from config import (
    DOMAIN_DEMAND_REFRESH_INTERVAL_MINUTES,
    DOMAIN_DEMAND_SHEET_ID,
    DOMAIN_DEMAND_SUMMARY_BY_GEO_TAB,
    DOMAIN_DEMAND_SUMMARY_TAB,
    KEITARO_HUB_CAMPAIGN_ID,
)

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
_CACHE_PATH = ROOT / "runtime" / "domain_demand_progress.json"
_refresh_lock = threading.Lock()
_refresh_running = False

# Auto-refresh interval for stale GET (minutes). Manual Refresh always rebuilds.
CACHE_STALE_MINUTES = int(DOMAIN_DEMAND_REFRESH_INTERVAL_MINUTES or 30)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _level_for_fill(pct: Optional[float]) -> str:
    if pct is None:
        return "none"
    if pct >= 90:
        return "green"
    if pct >= 50:
        return "yellow"
    return "red"


def _fnum(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(str(x).replace("%", "").replace(",", "").strip())
    except Exception:
        return None


def _read_cache() -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not _CACHE_PATH.is_file():
        return None, None
    try:
        raw = json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None, None
    if not isinstance(raw, dict):
        return None, None
    return raw, str(raw.get("cache_saved_utc") or raw.get("updated_at") or "") or None


def _write_cache(payload: Dict[str, Any]) -> None:
    _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    out = dict(payload)
    out["cache_saved_utc"] = _utc_now_iso()
    _CACHE_PATH.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")


def _cache_is_stale(data: Optional[Dict[str, Any]], saved: Optional[str]) -> bool:
    if not data or not saved:
        return True
    try:
        ts = saved.replace("Z", "+00:00")
        saved_dt = datetime.fromisoformat(ts)
        if saved_dt.tzinfo is None:
            saved_dt = saved_dt.replace(tzinfo=timezone.utc)
        age_m = (datetime.now(timezone.utc) - saved_dt).total_seconds() / 60.0
        return age_m >= CACHE_STALE_MINUTES
    except Exception:
        return True


def _summary_index(summary_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for r in summary_rows or []:
        scope = str(r.get("scope") or "").strip()
        family = str(r.get("family") or "").strip()
        feed = str(r.get("feed") or "").strip()
        if scope == "hub_total":
            out["hub"] = r
        elif scope == "family" and family:
            out[f"family:{family}"] = r
        elif scope == "family_feed" and family and feed:
            out[f"feed:{family}:{feed}"] = r
    return out


def _row_fill(r: Dict[str, Any]) -> Optional[float]:
    pct = _fnum(r.get("fill_pct"))
    if pct is not None:
        return pct
    demand = _fnum(r.get("demand_clicks")) or 0.0
    delivered = _fnum(r.get("delivered_clicks")) or 0.0
    if demand <= 0:
        return None
    return round(100.0 * delivered / demand, 1)


def _normalize_segments(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    segs: List[Dict[str, Any]] = []
    for r in rows or []:
        demand = int(round(_fnum(r.get("demand_clicks")) or 0))
        delivered = int(round(_fnum(r.get("delivered_clicks")) or 0))
        remaining = r.get("remaining")
        if remaining in (None, ""):
            remaining = max(0, demand - delivered)
        else:
            remaining = int(round(_fnum(remaining) or 0))
        pct = _row_fill(r)
        segs.append(
            {
                "geo": str(r.get("geo") or "").strip().lower(),
                "device": str(r.get("device") or "").strip().lower(),
                "demand_clicks": demand,
                "delivered_clicks": delivered,
                "remaining": remaining,
                "fill_pct": pct,
                "level": _level_for_fill(pct) if demand > 0 else "none",
                "trillion_campaign": str(r.get("trillion_campaign") or ""),
                "trillion_status": str(r.get("trillion_status") or ""),
                "trillion_hint": str(r.get("trillion_hint") or ""),
            }
        )
    segs.sort(key=lambda s: (s["geo"], s["device"]))
    return segs


def _totals_block(summary: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    def one(key: str) -> Dict[str, Any]:
        r = summary.get(key) or {}
        demand = int(round(_fnum(r.get("demand_clicks")) or 0))
        delivered = int(round(_fnum(r.get("delivered_clicks")) or 0))
        pct = _row_fill(r)
        return {
            "demand_clicks": demand,
            "delivered_clicks": delivered,
            "remaining": int(round(_fnum(r.get("remaining")) or max(0, demand - delivered))),
            "fill_pct": pct,
            "level": _level_for_fill(pct) if demand > 0 else "none",
        }

    feeds = []
    for key, r in sorted(summary.items()):
        if not key.startswith("feed:"):
            continue
        _pfx, family, feed = key.split(":", 2)
        block = one(key)
        block["family"] = family
        block["feed"] = feed
        feeds.append(block)

    return {
        "hub": one("hub"),
        "nipuhim": one("family:nipuhim"),
        "blend": one("family:blend"),
        "feeds": feeds,
    }


def _awaiting_payload(*, day: str, reason: str) -> Dict[str, Any]:
    hub_id = int(KEITARO_HUB_CAMPAIGN_ID)
    return {
        "status": "awaiting_daily",
        "updated_at": _utc_now_iso(),
        "calendar_day": day,
        "hub_campaign_id": hub_id,
        "reason": reason,
        "message": (
            "Domain demand bill is empty until the morning daily workflow rebuilds it "
            "(nightly rollover archived yesterday)."
        ),
        "totals": {
            "hub": {
                "demand_clicks": 0,
                "delivered_clicks": 0,
                "remaining": 0,
                "fill_pct": None,
                "level": "none",
            },
            "nipuhim": {
                "demand_clicks": 0,
                "delivered_clicks": 0,
                "remaining": 0,
                "fill_pct": None,
                "level": "none",
            },
            "blend": {
                "demand_clicks": 0,
                "delivered_clicks": 0,
                "remaining": 0,
                "fill_pct": None,
                "level": "none",
            },
            "feeds": [],
        },
        "segments": [],
        "errors": [],
        "sheet_id": (DOMAIN_DEMAND_SHEET_ID or "").strip(),
    }


def _ui_payload_from_raw(raw: Dict[str, Any], *, reason: str) -> Dict[str, Any]:
    day = str(raw.get("date") or "")
    if raw.get("error") == "empty_bill_refused" or (
        raw.get("status") == "error" and not raw.get("summary_by_geo")
    ):
        out = _awaiting_payload(day=day or "unknown", reason=reason)
        err = str(raw.get("error") or "empty_bill")
        out["errors"] = [err]
        out["logs"] = raw.get("logs") or []
        return out

    summary = _summary_index(list(raw.get("summary") or []))
    segments = _normalize_segments(list(raw.get("summary_by_geo") or []))
    totals = _totals_block(summary)
    errors: List[str] = []
    if raw.get("error"):
        errors.append(str(raw["error"]))
    write = raw.get("write")
    if isinstance(write, dict) and write.get("error"):
        errors.append(f"sheet write: {write['error']}")

    return {
        "status": "ok",
        "updated_at": str(raw.get("updated_at") or _utc_now_iso()),
        "calendar_day": day or None,
        "hub_campaign_id": int(raw.get("hub_campaign_id") or KEITARO_HUB_CAMPAIGN_ID),
        "hub_delivered_clicks": raw.get("hub_delivered_clicks"),
        "trillion_hint": raw.get("trillion_hint"),
        "reason": reason,
        "totals": totals,
        "segments": segments,
        "errors": errors,
        "logs": raw.get("logs") or [],
        "sheet_id": (DOMAIN_DEMAND_SHEET_ID or "").strip(),
        "summary_tab": DOMAIN_DEMAND_SUMMARY_TAB,
        "summary_by_geo_tab": DOMAIN_DEMAND_SUMMARY_BY_GEO_TAB,
    }


def build_domain_demand_progress_payload(*, reason: str = "api") -> Dict[str, Any]:
    """Refresh delivered clicks against today's bill (does not rebuild demand lines)."""
    from integrations.domain_demand import (
        build_domain_demand_payload,
        today_domain_demand_ready,
        _calendar_day,
    )

    day = _calendar_day()
    if not today_domain_demand_ready(date_str=day):
        return _awaiting_payload(day=day, reason=reason)

    raw = build_domain_demand_payload(rebuild_demand=False, reason=reason)
    return _ui_payload_from_raw(raw, reason=reason)


def refresh_domain_demand_progress(
    *,
    reason: str = "manual",
    resume_trillion: bool = True,
) -> Dict[str, Any]:
    """Force refresh: Keitaro delivered + write sheet; optionally resume underfilled Trillion."""
    global _refresh_running
    with _refresh_lock:
        if _refresh_running:
            cached, _saved = _read_cache()
            if cached:
                return {**cached, "refresh_skipped": True, "message": "refresh already running"}
        _refresh_running = True
    try:
        from config import DOMAIN_TRILLION_GUARD_ENABLED, KEYTR
        from integrations.domain_demand_guard import run_trillion_activate_for_demand
        from integrations.domain_demand import (
            sync_domain_demand,
            today_domain_demand_ready,
            _calendar_day,
        )

        day = _calendar_day()
        trillion_resume: Optional[Dict[str, Any]] = None
        if not today_domain_demand_ready(date_str=day):
            payload = _awaiting_payload(day=day, reason=reason)
        else:
            raw = sync_domain_demand(
                rebuild_demand=False, dry_run=False, reason=f"ui:{reason}"
            )
            if (
                resume_trillion
                and DOMAIN_TRILLION_GUARD_ENABLED
                and (KEYTR or "").strip()
            ):
                trillion_resume = run_trillion_activate_for_demand(
                    dry_run=False,
                    reason=f"ui:{reason}",
                    segments=raw.get("summary_by_geo"),
                )
                if int(trillion_resume.get("resumed") or 0) > 0:
                    raw = sync_domain_demand(
                        rebuild_demand=False,
                        dry_run=False,
                        reason=f"ui:{reason}_post_resume",
                    )
            payload = _ui_payload_from_raw(raw, reason=reason)
            if trillion_resume is not None:
                payload["trillion_resume"] = {
                    "resumed": trillion_resume.get("resumed"),
                    "errors": trillion_resume.get("errors") or [],
                    "actions": [
                        a
                        for a in (trillion_resume.get("actions") or [])
                        if str(a.get("status") or "") in ("resumed", "error")
                    ],
                }
        _write_cache(payload)
        return payload
    finally:
        with _refresh_lock:
            _refresh_running = False


def get_api_payload(*, allow_background_refresh: bool = True) -> Dict[str, Any]:
    data, saved = _read_cache()
    stale = _cache_is_stale(data, saved)
    out: Dict[str, Any] = {
        "cache_saved_utc": saved,
        "stale": stale,
    }
    if data:
        out.update(data)
    else:
        out.update(
            {
                "status": "missing",
                "calendar_day": None,
                "segments": [],
                "totals": {},
                "errors": ["No cached domain-demand progress yet"],
                "hub_campaign_id": int(KEITARO_HUB_CAMPAIGN_ID),
            }
        )

    if stale and allow_background_refresh:

        def _bg() -> None:
            try:
                refresh_domain_demand_progress(reason="stale_api", resume_trillion=True)
            except Exception as e:
                logger.exception("Background domain-demand progress refresh failed: %s", e)

        threading.Thread(target=_bg, daemon=True, name="domain-demand-progress").start()
        out["refresh_queued"] = True
    else:
        out["refresh_queued"] = False

    return out
