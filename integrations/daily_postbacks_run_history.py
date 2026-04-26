"""
Last-run snapshots per feed for the daily postbacks UI (dashboard + detail).

Separate from resume state in ``daily_conversion_postbacks_state.json``.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent


def last_run_path() -> Path:
    raw = (os.getenv("DAILY_POSTBACKS_LAST_RUN_PATH") or "").strip()
    if raw:
        return Path(raw)
    return ROOT / "runtime" / "daily_postbacks_last_run.json"


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


def postback_sources_enabled() -> list[str]:
    """Sources that would run under ``--only all`` when credentials exist (Kelkoo / Adexa / Yadore)."""
    from config import ADEXA_API_KEY, ADEXA_SITE_ID, FEED1_API_KEY, FEED2_API_KEY, YADORE_API_KEY

    out: list[str] = []
    if (FEED1_API_KEY or "").strip():
        out.append("kelkoo1")
    if (FEED2_API_KEY or "").strip():
        out.append("kelkoo2")
    if (ADEXA_SITE_ID or "").strip() and (ADEXA_API_KEY or "").strip():
        out.append("adexa")
    if (YADORE_API_KEY or "").strip():
        out.append("yadore")
    return out


_SOURCE_LABELS = {
    "kelkoo1": "Kelkoo feed 1",
    "kelkoo2": "Kelkoo feed 2",
    "adexa": "Adexa",
    "yadore": "Yadore",
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
        }

    data = load_last_runs()
    sent: list[str] = []
    pending: list[str] = []

    for src in scheduled:
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

    if len(sent) == len(scheduled):
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
    }
