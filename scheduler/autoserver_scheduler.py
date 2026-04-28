"""
APScheduler hourly broadcast for AutoServer-derived automations (minute 0).

Start via ``start_autoserver_scheduler()`` from the Flask app factory / module
load so it runs under Gunicorn (not only ``__main__``).

For multi-worker Gunicorn, set ``AUTOSERVER_SCHEDULER_ENABLED=0`` on all but one worker.
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, List, Optional

logger = logging.getLogger(__name__)

_automation_listeners: List[Any] = []
_scheduler: Any = None
_started = False


def register_automation(automation_instance: Any) -> None:
    _automation_listeners.append(automation_instance)
    logger.info("Registered automation: %s", automation_instance.__class__.__name__)


def get_automation_listeners() -> List[Any]:
    return list(_automation_listeners)


def get_scheduler() -> Any:
    return _scheduler


def scheduler_running() -> bool:
    sch = _scheduler
    return bool(sch and getattr(sch, "running", False))


def _hourly_signal_broadcast() -> None:
    current_hour = datetime.now().hour
    logger.info("=== AUTOSERVER HOURLY SIGNAL hour=%s ===", current_hour)
    for automation in _automation_listeners:
        try:
            automation.on_hourly_signal(current_hour)
        except Exception:
            logger.exception("Error in %s", automation.__class__.__name__)


def _run_all_automations_job() -> None:
    """Manual \"trigger all\": run each automation's ``run_manually`` (full run, not hour-gated)."""
    logger.info("=== AUTOSERVER MANUAL ALL (run_manually each) ===")
    for automation in _automation_listeners:
        try:
            automation.run_manually()
        except Exception:
            logger.exception("Error in %s", automation.__class__.__name__)


def _run_single_automation_job(automation: Any) -> None:
    try:
        logger.info("=== AUTOSERVER MANUAL SINGLE: %s ===", automation.__class__.__name__)
        automation.run_manually()
    except Exception:
        logger.exception("Error running %s", automation.__class__.__name__)


def ensure_automations_initialized() -> None:
    """Register automation instances (idempotent). Safe to call from API before scheduler start."""
    _ensure_listeners()


def _ensure_listeners() -> None:
    if _automation_listeners:
        return
    from integrations.autoserver.env import ensure_autoserver_env

    ensure_autoserver_env()
    from automations.autoserver import setup_automations

    setup_automations(register_automation)


def _trigger_background(fn: Callable[..., None], *args: Any) -> None:
    sch = _scheduler
    if sch is not None and getattr(sch, "running", False):
        sch.add_job(fn, trigger="date", run_date=datetime.now(), args=list(args))
    else:
        threading.Thread(target=lambda: fn(*args), name="autoserver-manual", daemon=True).start()


def _parse_utc_iso(raw: str) -> Optional[datetime]:
    s = str(raw or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


def _should_schedule_startup_catchup() -> bool:
    """
    Run one immediate catch-up when the process starts mid-hour and no recent
    AutoServer execution was recorded.

    Without this, a deploy/restart at e.g. 09:58 leaves the dashboard on
    "No run yet" until 10:00 even though the scheduler is healthy.
    """
    now_utc = datetime.now(timezone.utc)
    if now_utc.minute == 0:
        return False
    try:
        from automations.autoserver.run_log import read_entries_newest_first

        latest = read_entries_newest_first(limit=1)
    except Exception:
        latest = []
    if not latest:
        return True
    finished = _parse_utc_iso(str((latest[0] or {}).get("finished_at") or ""))
    if finished is None:
        return True
    return finished < (now_utc - timedelta(minutes=55))


def schedule_trigger_all() -> None:
    _ensure_listeners()
    _trigger_background(_run_all_automations_job)


def schedule_trigger_one(automation: Any) -> None:
    _trigger_background(_run_single_automation_job, automation)


def start_autoserver_scheduler() -> None:
    global _scheduler, _started
    from config import AUTOSERVER_SCHEDULER_ENABLED

    if not AUTOSERVER_SCHEDULER_ENABLED:
        logger.info("AutoServer APScheduler disabled (AUTOSERVER_SCHEDULER_ENABLED)")
        return
    if os.getenv("FLASK_DEBUG") == "1" and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return
    if _started:
        return

    from apscheduler.schedulers.background import BackgroundScheduler

    _ensure_listeners()
    _scheduler = BackgroundScheduler()
    _scheduler.add_job(_hourly_signal_broadcast, trigger="cron", minute=0)
    _scheduler.start()
    if _should_schedule_startup_catchup():
        _scheduler.add_job(_hourly_signal_broadcast, trigger="date", run_date=datetime.now())
        logger.info("AutoServer APScheduler scheduled startup catch-up run")
    _started = True
    logger.info("AutoServer APScheduler started (hourly at :00)")


def stop_autoserver_scheduler(wait: bool = False) -> None:
    global _scheduler, _started
    sch = _scheduler
    if sch is None:
        return
    try:
        sch.shutdown(wait=wait)
    except Exception as e:
        logger.warning("AutoServer scheduler shutdown: %s", e)
    _scheduler = None
    _started = False
