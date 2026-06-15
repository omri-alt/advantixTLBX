"""
Last-run snapshot for SK exploration WL sync (homepage banner).

Written after each live ``sync_exploration_wl_from_keitaro_sales`` run.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent


def last_run_path() -> Path:
    raw = (os.getenv("SK_EXPLORATION_WL_SYNC_LAST_RUN_PATH") or "").strip()
    if raw:
        return Path(raw)
    return ROOT / "runtime" / "sk_exploration_wl_sync_last_run.json"


def _sync_tz_name() -> str:
    from config import SK_EXPLORATION_WL_SYNC_TZ

    return (SK_EXPLORATION_WL_SYNC_TZ or "Asia/Jerusalem").strip() or "Asia/Jerusalem"


def _calendar_day_local() -> str:
    from zoneinfo import ZoneInfo

    try:
        tz = ZoneInfo(_sync_tz_name())
    except Exception:
        tz = timezone.utc
    return datetime.now(tz).strftime("%Y-%m-%d")


def load_last_run() -> Optional[Dict[str, Any]]:
    p = last_run_path()
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception as e:
        logger.warning("SK WL sync last-run read failed %s: %s", p, e)
        return None


def record_last_run(summary: Dict[str, Any]) -> None:
    """Persist a live sync summary for the Control Center banner."""
    if summary.get("dry_run"):
        return

    qw = summary.get("quality_wl") if isinstance(summary.get("quality_wl"), dict) else {}
    payload = {
        "at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "calendar_day": _calendar_day_local(),
        "timezone": _sync_tz_name(),
        "dry_run": False,
        "ok": True,
        "sources_appended": int(summary.get("sources_appended") or 0),
        "sources_reactivated": int(summary.get("sources_reactivated") or 0),
        "campaigns_updated": int(summary.get("campaigns_updated") or 0),
        "quality_wl_appended": int(qw.get("appended") or 0),
        "lookback_days": int(summary.get("lookback_days") or 0),
    }

    p = last_run_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def wl_sync_banner_payload_for_today() -> Dict[str, Any]:
    """
    Homepage banner when today's live sync added new converting sources.

    Uses the automation timezone calendar day (default ``Asia/Jerusalem``).
    """
    today = _calendar_day_local()
    entry = load_last_run()
    base: Dict[str, Any] = {
        "show": False,
        "calendar_day": today,
        "timezone": _sync_tz_name(),
        "message": "",
        "sources_appended": 0,
        "quality_wl_appended": 0,
        "campaigns_updated": 0,
    }
    if not entry or entry.get("dry_run") or not entry.get("ok"):
        return base

    if str(entry.get("calendar_day") or "") != today:
        return base

    sources = int(entry.get("sources_appended") or 0)
    qw = int(entry.get("quality_wl_appended") or 0)
    campaigns = int(entry.get("campaigns_updated") or 0)

    if sources <= 0 and qw <= 0:
        return base

    parts = []
    if sources > 0:
        parts.append(
            f"{sources} new converting source{'s' if sources != 1 else ''} added to exploration WL"
        )
    if qw > 0:
        parts.append(f"{qw} new QualityWL row{'s' if qw != 1 else ''}")
    detail = " · ".join(parts)
    if campaigns > 0:
        detail += f" ({campaigns} campaign{'s' if campaigns != 1 else ''})"

    base.update(
        {
            "show": True,
            "level": "new",
            "message": f"SK WL sync today: {detail}.",
            "sources_appended": sources,
            "quality_wl_appended": qw,
            "campaigns_updated": campaigns,
            "ran_at_utc": entry.get("at_utc"),
        }
    )
    return base
