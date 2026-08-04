"""
Last-run snapshots per feed for the daily postbacks UI (dashboard + detail).

Separate from resume state in ``daily_conversion_postbacks_state.json``.
"""
from __future__ import annotations

import json
import logging
import os
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent

_running_lock = threading.Lock()

# Drop stale "running" markers so a crashed worker does not leave yellow forever.
_RUNNING_STALE_SECONDS = 6 * 3600


def last_run_path() -> Path:
    raw = (os.getenv("DAILY_POSTBACKS_LAST_RUN_PATH") or "").strip()
    if raw:
        return Path(raw)
    return ROOT / "runtime" / "daily_postbacks_last_run.json"


def running_path() -> Path:
    raw = (os.getenv("DAILY_POSTBACKS_RUNNING_PATH") or "").strip()
    if raw:
        return Path(raw)
    return ROOT / "runtime" / "daily_postbacks_running.json"


def load_last_runs() -> Dict[str, Any]:
    p = last_run_path()
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning("daily postbacks last-run read failed %s: %s", p, e)
        return {}


def record_last_run(
    target: str,
    report_date: str,
    *,
    dry_run: bool,
    ok: bool,
    summary: Dict[str, Any],
    batch_exit_code: int,
) -> None:
    p = last_run_path()
    data = load_last_runs()
    data[target.strip().lower()] = {
        "at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "report_date": report_date,
        "dry_run": bool(dry_run),
        "ok": bool(ok),
        "exit_code": int(batch_exit_code),
        "summary": summary,
    }
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or _utc_now()
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc(s: str) -> Optional[datetime]:
    raw = (s or "").strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        return datetime.fromisoformat(raw).astimezone(timezone.utc)
    except Exception:
        return None


def load_running() -> Dict[str, Any]:
    p = running_path()
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning("daily postbacks running-state read failed %s: %s", p, e)
        return {}


def _save_running(data: Dict[str, Any]) -> None:
    p = running_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def mark_feed_running(
    target: str,
    *,
    report_date: str,
    triggered_by: str = "ui",
) -> None:
    key = target.strip().lower()
    if not key:
        return
    with _running_lock:
        data = load_running()
        data[key] = {
            "started_at_utc": _utc_iso(),
            "report_date": report_date,
            "triggered_by": (triggered_by or "ui").strip() or "ui",
        }
        _save_running(data)


def clear_feed_running(target: str) -> None:
    key = target.strip().lower()
    if not key:
        return
    with _running_lock:
        data = load_running()
        if key in data:
            del data[key]
            _save_running(data)


@contextmanager
def feed_run_marker(
    target: str,
    *,
    report_date: str,
    triggered_by: str = "ui",
) -> Iterator[None]:
    """Mark a feed as running for the dashboard (yellow), clear on exit."""
    mark_feed_running(target, report_date=report_date, triggered_by=triggered_by)
    try:
        yield
    finally:
        clear_feed_running(target)


def is_feed_running(target: str, *, now: Optional[datetime] = None) -> bool:
    key = target.strip().lower()
    entry = load_running().get(key)
    if not isinstance(entry, dict):
        return False
    started = _parse_utc(str(entry.get("started_at_utc") or ""))
    if started is None:
        return True
    age = ((_utc_now() if now is None else now) - started).total_seconds()
    return age <= _RUNNING_STALE_SECONDS


def feed_live_run_succeeded_today(
    target: str,
    *,
    report_date: Optional[str] = None,
    calendar_day_utc: Optional[str] = None,
) -> bool:
    """
    True when a non-dry-run successful batch for this feed finished on the UTC calendar day
    (and optionally for the given report_date). Used to skip the 09:00 scheduler if the
    operator already ran postbacks manually earlier.

    Partial Kelkoo runs (``summary.partial`` / non-empty ``pending_geos``) do **not** count
    as success so hourly retries for missing countries keep going.
    """
    entry = load_last_runs().get(target.strip().lower())
    if not isinstance(entry, dict):
        return False
    if entry.get("dry_run") or not entry.get("ok"):
        return False
    today = calendar_day_utc or _utc_now().strftime("%Y-%m-%d")
    if str(entry.get("at_utc") or "")[:10] != today:
        return False
    if report_date and str(entry.get("report_date") or "") != str(report_date):
        return False
    summ = entry.get("summary")
    if isinstance(summ, dict):
        if summ.get("partial"):
            return False
        pending = summ.get("pending_geos") or summ.get("not_ready")
        if isinstance(pending, (list, tuple)) and len(pending) > 0:
            return False
    return True


def postback_sources_enabled() -> list[str]:
    """Sources that would run under ``--only all`` when credentials exist (Kelkoo / Adexa / Yadore / Effinity)."""
    from config import (
        ADEXA_API_KEY,
        ADEXA_SITE_ID,
        EFFINITY_API_KEY,
        FEED1_API_KEY,
        FEED2_API_KEY,
        FEED5_API_KEY,
        YADORE_API_KEY,
        shopnomix_reporting_enabled,
    )

    out: list[str] = []
    if (FEED1_API_KEY or "").strip():
        out.append("kelkoo1")
    if (FEED2_API_KEY or "").strip():
        out.append("kelkoo2")
    if (FEED5_API_KEY or "").strip():
        out.append("kelkoo5")
    if (ADEXA_SITE_ID or "").strip() and (ADEXA_API_KEY or "").strip():
        out.append("adexa")
    if (YADORE_API_KEY or "").strip():
        out.append("yadore")
        out.append("yadore_sales")
    if (EFFINITY_API_KEY or "").strip():
        out.append("effinity")
    if shopnomix_reporting_enabled():
        out.append("shopnomix")
    return out


_SOURCE_LABELS = {
    "kelkoo1": "Kelkoo feed 1",
    "kelkoo2": "Kelkoo feed 2",
    "kelkoo5": "Kelkoo feed 5",
    "adexa": "Adexa",
    "yadore": "Yadore (clicks)",
    "yadore_sales": "Yadore (sales)",
    "effinity": "Effinity (MTD sales)",
    "shopnomix": "Shopnomix feed6 (tile + coupons)",
}


def feed_today_status(
    target: str,
    *,
    calendar_day_utc: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Per-feed traffic-light for the postbacks hub (mirrors homepage banner semantics).

    ``running`` (yellow) | ``ok`` (green) | ``none`` (red).
    """
    today = calendar_day_utc or _utc_now().strftime("%Y-%m-%d")
    key = target.strip().lower()
    running_map = load_running()
    if is_feed_running(key):
        run = running_map.get(key) if isinstance(running_map.get(key), dict) else {}
        return {
            "key": key,
            "level": "running",
            "message": "Running…",
            "calendar_day_utc": today,
            "report_date": (run or {}).get("report_date"),
            "started_at_utc": (run or {}).get("started_at_utc"),
        }

    entry = load_last_runs().get(key)
    ok_today = feed_live_run_succeeded_today(key, calendar_day_utc=today)
    if ok_today:
        return {
            "key": key,
            "level": "ok",
            "message": f"Ran successfully — {today}",
            "calendar_day_utc": today,
            "report_date": (entry or {}).get("report_date") if isinstance(entry, dict) else None,
            "at_utc": (entry or {}).get("at_utc") if isinstance(entry, dict) else None,
        }

    failed_today = False
    if isinstance(entry, dict) and not entry.get("dry_run"):
        at = str(entry.get("at_utc") or "")[:10]
        if at == today and entry.get("ok") is False:
            failed_today = True

    return {
        "key": key,
        "level": "none",
        "message": "Failed today" if failed_today else "Didn't run today",
        "calendar_day_utc": today,
        "report_date": (entry or {}).get("report_date") if isinstance(entry, dict) else None,
        "at_utc": (entry or {}).get("at_utc") if isinstance(entry, dict) else None,
        "failed_today": failed_today,
    }


def postback_feeds_status_payload() -> Dict[str, Any]:
    """JSON for dashboard polling: per-feed yellow/green/red status."""
    today = _utc_now().strftime("%Y-%m-%d")
    feeds: List[Dict[str, Any]] = []
    any_running = False
    for src in postback_sources_enabled():
        st = feed_today_status(src, calendar_day_utc=today)
        if st.get("level") == "running":
            any_running = True
        feeds.append(st)
    return {
        "calendar_day_utc": today,
        "any_running": any_running,
        "feeds": feeds,
    }


def postback_banner_payload_for_today() -> Dict[str, Any]:
    """
    Homepage banner: which daily conversion postback sources finished a **non-dry-run**
    successful batch on the current **UTC calendar day**, from ``daily_postbacks_last_run.json``.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    scheduled = postback_sources_enabled()
    if not scheduled:
        return {
            "calendar_day_utc": today,
            "level": "none",
            "message": "No postback sources configured.",
            "scheduled": [],
            "sent": [],
            "pending": [],
            "feeds": [],
            "any_running": False,
        }

    data = load_last_runs()
    sent: list[str] = []
    pending: list[str] = []
    feed_statuses: List[Dict[str, Any]] = []
    any_running = False

    for src in scheduled:
        st = feed_today_status(src, calendar_day_utc=today)
        feed_statuses.append(st)
        if st.get("level") == "running":
            any_running = True
            pending.append(src)
            continue
        entry = data.get(src)
        ok = bool(
            isinstance(entry, dict)
            and not entry.get("dry_run")
            and entry.get("ok")
        )
        at = str((entry or {}).get("at_utc") or "")[:10] if isinstance(entry, dict) else ""
        if ok and at == today:
            sent.append(src)
        else:
            pending.append(src)

    if any_running:
        level = "running"
        running_labels = ", ".join(
            _SOURCE_LABELS.get(s["key"], s["key"])
            for s in feed_statuses
            if s.get("level") == "running"
        )
        msg = f"Postbacks running: {running_labels}"
        if sent:
            msg += " — " + ", ".join(_SOURCE_LABELS.get(s, s) for s in sent) + " already sent today"
    elif len(sent) == len(scheduled):
        level = "all"
        msg = "All postbacks sent today"
    elif not sent:
        level = "none"
        msg = "No postbacks sent yet today"
    else:
        level = "partial"
        sl = ", ".join(_SOURCE_LABELS.get(s, s) for s in sent)
        pl = ", ".join(_SOURCE_LABELS.get(s, s) for s in pending)
        msg = f"Partial: {sl} sent — {pl} not yet sent today"

    return {
        "calendar_day_utc": today,
        "level": level,
        "message": msg,
        "scheduled": scheduled,
        "sent": sent,
        "pending": pending,
        "feeds": feed_statuses,
        "any_running": any_running,
    }
