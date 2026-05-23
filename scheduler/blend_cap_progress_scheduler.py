"""
Refresh Blend geo × device click-cap progress cache every N hours (default 3).
"""
from __future__ import annotations

import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

_scheduler: Any = None
_started = False


def _enabled() -> bool:
    raw = (os.getenv("BLEND_CAP_PROGRESS_SCHEDULER_ENABLED") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def start_blend_cap_progress_scheduler() -> None:
    global _scheduler, _started
    if _started or not _enabled():
        return
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
    except ImportError:
        logger.warning("APScheduler not installed; Blend cap progress scheduler disabled")
        return

    from integrations.blend_cap_progress import refresh_blend_cap_progress, refresh_interval_hours

    hours = refresh_interval_hours()
    _scheduler = BackgroundScheduler()

    def _job() -> None:
        try:
            refresh_blend_cap_progress(reason="scheduler")
        except Exception as e:
            logger.exception("Blend cap progress scheduler job failed: %s", e)

    _scheduler.add_job(
        _job,
        "interval",
        hours=hours,
        id="blend_cap_progress_refresh",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    _scheduler.start()
    _started = True
    logger.info("Blend cap progress scheduler started (every %.1f h)", hours)

    try:
        refresh_blend_cap_progress(reason="startup")
    except Exception as e:
        logger.warning("Blend cap progress startup refresh failed: %s", e)
