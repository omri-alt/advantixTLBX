"""
APScheduler jobs for AutoServer-derived automations.

Each automation has its own hourly cron job (minute 0) so a slow Ecomnia track run does not
block KLWL / Blend sync on the same worker tick.

Start via ``start_autoserver_scheduler()`` from ``scheduler.background`` (one Gunicorn
worker when using ``gunicorn.conf.py``).
"""
from __future__ import annotations

import json
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


def _heartbeat_path() -> "Path":
    from pathlib import Path

    from config import AUTOSERVER_SCHEDULER_HEARTBEAT_PATH

    p = Path(AUTOSERVER_SCHEDULER_HEARTBEAT_PATH)
    if not p.is_absolute():
        p = Path(__file__).resolve().parents[1] / p
    return p


def _write_scheduler_heartbeat() -> None:
    path = _heartbeat_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "pid": os.getpid(),
        "at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "running": bool(_scheduler and getattr(_scheduler, "running", False)),
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    tmp.replace(path)


def _heartbeat_is_recent(max_age_minutes: int = 90) -> bool:
    path = _heartbeat_path()
    if not path.exists():
        return False
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        at_s = str(data.get("at_utc") or "")
        if at_s.endswith("Z"):
            at_s = at_s[:-1] + "+00:00"
        at = datetime.fromisoformat(at_s).astimezone(timezone.utc)
        return at >= datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
    except Exception:
        return False


def scheduler_running() -> bool:
    sch = _scheduler
    if sch and getattr(sch, "running", False):
        return True
    return _heartbeat_is_recent()


def _current_scheduler_hour() -> int:
    from config import AUTOSERVER_SCHEDULER_TZ

    from zoneinfo import ZoneInfo

    tz_name = (AUTOSERVER_SCHEDULER_TZ or "UTC").strip()
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        logger.warning("Invalid AUTOSERVER_SCHEDULER_TZ %r; using UTC", tz_name)
        tz = ZoneInfo("UTC")
    return datetime.now(tz).hour


def _run_automation_hourly(automation: Any) -> None:
    hour = _current_scheduler_hour()
    name = automation.__class__.__name__
    logger.info("=== AUTOSERVER scheduled %s (hour=%s tz) ===", name, hour)
    try:
        automation.on_hourly_signal(hour)
    except Exception:
        logger.exception("Error in %s.on_hourly_signal", name)


def _run_close_nipuhim_scheduled() -> None:
    """Daily Zeropark ``pause_generalMehila`` at ``ZEROPARK_CLOSE_*`` in ``ZEROPARK_CLOSE_TZ``."""
    _ensure_listeners()
    for automation in _automation_listeners:
        if automation.__class__.__name__ == "CloseNipuhimAuto":
            logger.info("=== CloseNipuhimAuto cron (Zeropark generalMehila pause) ===")
            automation._wrap_run("scheduler", automation._execute)
            return
    logger.warning("CloseNipuhimAuto not registered; skip Zeropark close cron")


def _hourly_signal_broadcast() -> None:
    """Legacy single-threaded broadcast (manual catch-up / trigger-all internals)."""
    hour = _current_scheduler_hour()
    logger.info("=== AUTOSERVER HOURLY SIGNAL hour=%s ===", hour)
    for automation in _automation_listeners:
        try:
            automation.on_hourly_signal(hour)
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


def _run_single_automation_action_job(automation: Any, action: str) -> None:
    try:
        logger.info(
            "=== AUTOSERVER MANUAL ACTION: %s (%s) ===",
            automation.__class__.__name__,
            action,
        )
        automation.run_manual_action(action)
    except Exception:
        logger.exception(
            "Error running %s action %s",
            automation.__class__.__name__,
            action,
        )


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
    Optional deploy catch-up (off by default — parallel runs can overload the
    scheduler worker). Set ``AUTOSERVER_STARTUP_CATCHUP=1`` to enable.
    """
    flag = (os.getenv("AUTOSERVER_STARTUP_CATCHUP") or "").strip().lower()
    if flag not in ("1", "true", "yes", "on"):
        return False
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


def schedule_trigger_action(automation: Any, action: str) -> None:
    _trigger_background(_run_single_automation_action_job, automation, action)


def start_autoserver_scheduler() -> None:
    global _scheduler, _started
    from config import AUTOSERVER_SCHEDULER_ENABLED

    if not AUTOSERVER_SCHEDULER_ENABLED:
        logger.info("AutoServer APScheduler disabled (AUTOSERVER_SCHEDULER_ENABLED)")
        return
    if os.getenv("FLASK_DEBUG") == "1" and os.environ.get("WERKZEUG_RUN_MAIN") == "false":
        return
    if _started:
        return

    from apscheduler.schedulers.background import BackgroundScheduler

    from config import (
        TRILLION_BLEND_CAP_GUARD_INTERVAL_MINUTES,
        ZEROPARK_CLOSE_HOUR,
        ZEROPARK_CLOSE_MINUTE,
        ZEROPARK_CLOSE_TZ,
    )

    _ensure_listeners()
    _scheduler = BackgroundScheduler()
    for automation in _automation_listeners:
        name = automation.__class__.__name__
        if name == "CloseNipuhimAuto":
            continue
        trigger_kwargs: dict[str, Any] = {"minute": 0}
        job_id = f"autoserver_hourly_{name}"
        if name == "BlendTrCapGuard":
            interval_m = int(TRILLION_BLEND_CAP_GUARD_INTERVAL_MINUTES)
            trigger_kwargs = (
                {"minute": 0}
                if interval_m >= 60
                else {"minute": f"*/{interval_m}"}
            )
            job_id = f"autoserver_interval_{name}"
        _scheduler.add_job(
            _run_automation_hourly,
            trigger="cron",
            args=[automation],
            id=job_id,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=900,
            **trigger_kwargs,
        )
    try:
        from zoneinfo import ZoneInfo

        zp_close_tz = ZoneInfo(ZEROPARK_CLOSE_TZ or "Europe/Warsaw")
    except Exception:
        logger.warning("Invalid ZEROPARK_CLOSE_TZ %r; using Europe/Warsaw", ZEROPARK_CLOSE_TZ)
        from zoneinfo import ZoneInfo

        zp_close_tz = ZoneInfo("Europe/Warsaw")
    _scheduler.add_job(
        _run_close_nipuhim_scheduled,
        trigger="cron",
        hour=int(ZEROPARK_CLOSE_HOUR),
        minute=int(ZEROPARK_CLOSE_MINUTE),
        timezone=zp_close_tz,
        id="zeropark_close_general_mehila",
        replace_existing=True,
        max_instances=1,
    )
    _scheduler.add_job(
        _write_scheduler_heartbeat,
        trigger="interval",
        minutes=5,
        id="autoserver_scheduler_heartbeat",
        replace_existing=True,
    )
    _scheduler.start()
    _write_scheduler_heartbeat()
    if _should_schedule_startup_catchup():
        for automation in _automation_listeners:
            if automation.__class__.__name__ == "CloseNipuhimAuto":
                continue
            _scheduler.add_job(
                _run_automation_hourly,
                trigger="date",
                run_date=datetime.now(),
                args=[automation],
                id=f"autoserver_catchup_{automation.__class__.__name__}",
            )
        logger.info("AutoServer APScheduler scheduled per-automation startup catch-up")
    _started = True
    logger.info(
        (
            "AutoServer APScheduler started (%s scheduled jobs; "
            "BlendTrCapGuard every %d minutes; "
            "Zeropark close at %02d:%02d %s)"
        ),
        len(_automation_listeners) - 1,
        int(TRILLION_BLEND_CAP_GUARD_INTERVAL_MINUTES),
        int(ZEROPARK_CLOSE_HOUR),
        int(ZEROPARK_CLOSE_MINUTE),
        ZEROPARK_CLOSE_TZ,
    )


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
