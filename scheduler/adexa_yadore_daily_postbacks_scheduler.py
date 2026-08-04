"""
Scheduled Adexa + Yadore daily conversion postbacks (12:30 Asia/Jerusalem by default).

Runs ``adexa`` and ``yadore`` (click) feeds for yesterday UTC. Skips a feed when a
successful non-dry-run already finished today (manual early run). Yadore sales stay
on ``YADORE_SALES_SCHEDULER_*`` (default 10:00).
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_started = False
_run_lock = threading.Lock()

_DEFAULT_TARGETS = ("adexa", "yadore")


def enabled_adexa_yadore_postback_feeds() -> List[str]:
    from config import ADEXA_API_KEY, ADEXA_SITE_ID, YADORE_API_KEY

    out: List[str] = []
    if (ADEXA_SITE_ID or "").strip() and (ADEXA_API_KEY or "").strip():
        out.append("adexa")
    if (YADORE_API_KEY or "").strip():
        out.append("yadore")
    return out


def _seconds_until_local(hour: int, minute: int, tz_name: str) -> float:
    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return max(1.0, (target - now).total_seconds())


def run_adexa_yadore_daily_postbacks_scheduled(
    *,
    report_date: Optional[str] = None,
    dry_run: bool = False,
    triggered_by: str = "cron",
    feeds: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """
    Run Adexa + Yadore click postbacks for ``report_date`` (default: yesterday UTC).

    Skips feeds that already recorded a successful live run today for that date.
    """
    from config import (
        ADEXA_YADORE_DAILY_POSTBACK_RUN_LOG_MAX,
        ADEXA_YADORE_DAILY_POSTBACK_RUN_LOG_PATH,
        DAILY_CONVERSION_POSTBACK_STATE_PATH,
    )
    from integrations.daily_conversion_postbacks import (
        default_report_date_str,
        run_daily_conversion_postbacks_batch,
    )
    from integrations.daily_postbacks_run_history import feed_live_run_succeeded_today
    from integrations.kelkoo_daily_postbacks_run_log import append_run_log

    started = datetime.now(timezone.utc)
    date_s = (report_date or default_report_date_str()).strip()
    feed_list = [
        f.strip().lower()
        for f in (feeds or enabled_adexa_yadore_postback_feeds())
        if str(f).strip().lower() in _DEFAULT_TARGETS
    ]
    # Preserve configured order: adexa then yadore.
    feed_list = [t for t in _DEFAULT_TARGETS if t in feed_list]

    if not feed_list:
        out = {
            "ok": False,
            "error": "No Adexa/Yadore postback feeds configured (API keys)",
            "report_date": date_s,
            "triggered_by": triggered_by,
        }
        append_run_log(
            {**out, "started_at": started.strftime("%Y-%m-%dT%H:%M:%SZ")},
            log_path=ADEXA_YADORE_DAILY_POSTBACK_RUN_LOG_PATH,
            max_entries=int(ADEXA_YADORE_DAILY_POSTBACK_RUN_LOG_MAX),
        )
        return out

    if not _run_lock.acquire(blocking=False):
        logger.warning("Adexa/Yadore daily postbacks: another run in progress; skipping")
        return {
            "ok": False,
            "error": "another_run_in_progress",
            "report_date": date_s,
            "triggered_by": triggered_by,
        }

    results: Dict[str, Any] = {}
    skipped: List[str] = []
    try:
        for feed in feed_list:
            if not dry_run and feed_live_run_succeeded_today(feed, report_date=date_s):
                logger.info(
                    "Adexa/Yadore daily postbacks %s %s: already succeeded today — skipping",
                    feed,
                    date_s,
                )
                skipped.append(feed)
                results[feed] = {"ok": True, "skipped_already_ran_today": True}
                continue

            logger.info(
                "Adexa/Yadore daily postbacks: running %s for %s (triggered_by=%s dry_run=%s)",
                feed,
                date_s,
                triggered_by,
                dry_run,
            )
            batch = run_daily_conversion_postbacks_batch(
                report_date=date_s,
                only=feed,
                only_geo=None,
                dry_run=dry_run,
                no_resume=False,
                reset_sources=None,
            )
            summ = None
            for row in batch.get("results") or []:
                if str(row.get("target") or "").strip().lower() == feed:
                    summ = row.get("summary") or {}
                    break
            if summ is None:
                summ = {"ok": bool(batch.get("ok")), "error": batch.get("error")}
            results[feed] = summ

        finished = datetime.now(timezone.utc)
        ok = all(bool((results.get(f) or {}).get("ok", True)) for f in feed_list)
        out = {
            "ok": ok,
            "report_date": date_s,
            "triggered_by": triggered_by,
            "dry_run": bool(dry_run),
            "feeds": results,
            "skipped_already_ran_today": skipped,
            "state_path": str(Path(DAILY_CONVERSION_POSTBACK_STATE_PATH)),
            "started_at": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "finished_at": finished.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        try:
            append_run_log(
                out,
                log_path=ADEXA_YADORE_DAILY_POSTBACK_RUN_LOG_PATH,
                max_entries=int(ADEXA_YADORE_DAILY_POSTBACK_RUN_LOG_MAX),
            )
        except Exception:
            logger.exception("Adexa/Yadore daily postbacks: run log append failed")
        logger.info(
            "Adexa/Yadore daily postbacks finished: date=%s ok=%s skipped=%s",
            date_s,
            ok,
            skipped,
        )
        return out
    finally:
        _run_lock.release()


def _thread_loop() -> None:
    from config import (
        ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_HOUR_LOCAL,
        ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_MINUTE,
        ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_TZ,
    )

    while True:
        try:
            delay = _seconds_until_local(
                int(ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_HOUR_LOCAL),
                int(ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_MINUTE),
                ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_TZ,
            )
            logger.info(
                "Adexa/Yadore daily postbacks scheduler: sleeping %.0fs until %02d:%02d %s",
                delay,
                int(ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_HOUR_LOCAL),
                int(ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_MINUTE),
                ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_TZ,
            )
            time.sleep(delay)
            run_adexa_yadore_daily_postbacks_scheduled(triggered_by="cron")
        except Exception:
            logger.exception("Adexa/Yadore daily postbacks scheduler loop failed")
            time.sleep(60)


def start_adexa_yadore_daily_postbacks_scheduler() -> None:
    """Start thread fallback when AutoServer APScheduler is not owning the cron."""
    global _started
    from config import (
        AUTOSERVER_SCHEDULER_ENABLED,
        ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_ENABLED,
        ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_HOUR_LOCAL,
        ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_MINUTE,
        ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_TZ,
    )

    if not ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_ENABLED:
        logger.info(
            "Adexa/Yadore daily postbacks scheduler disabled "
            "(ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_ENABLED=0)"
        )
        return
    if not enabled_adexa_yadore_postback_feeds():
        logger.info("Adexa/Yadore daily postbacks scheduler skipped (no API keys)")
        return
    if AUTOSERVER_SCHEDULER_ENABLED:
        logger.info(
            "Adexa/Yadore daily postbacks use APScheduler cron (AutoServer scheduler); "
            "thread loop skipped"
        )
        return
    if os.getenv("FLASK_DEBUG") == "1" and os.environ.get("WERKZEUG_RUN_MAIN") == "false":
        return
    if _started:
        return
    threading.Thread(
        target=_thread_loop, name="adexa-yadore-daily-postbacks-scheduler", daemon=True
    ).start()
    _started = True
    logger.info(
        "Adexa/Yadore daily postbacks scheduler started (daily %02d:%02d %s)",
        int(ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_HOUR_LOCAL),
        int(ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_MINUTE),
        ADEXA_YADORE_DAILY_POSTBACK_SCHEDULER_TZ,
    )
