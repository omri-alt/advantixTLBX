from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_started = False


def _seconds_until_local_time(hour_local: int, minute_local: int, tz_name: str) -> float:
    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)
    target = now.replace(hour=hour_local, minute=minute_local, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return max(1.0, (target - now).total_seconds())


def _run_once() -> None:
    from integrations.blend_cpc_refresh import refresh_blend_cpcs

    res = refresh_blend_cpcs(dry_run=False)
    if res.get("ok"):
        logger.info(
            "Blend CPC refresh done: window=%s..%s rows=%s cpc_changed=%s weights_changed=%s",
            ((res.get("date_window") or {}).get("from") or ""),
            ((res.get("date_window") or {}).get("to") or ""),
            res.get("row_count"),
            res.get("changed_cpc_rows"),
            res.get("changed_weight_rows"),
        )
    else:
        logger.error("Blend CPC refresh failed")


def _loop() -> None:
    from config import (
        BLEND_CPC_REFRESH_SCHEDULER_HOUR_LOCAL,
        BLEND_CPC_REFRESH_SCHEDULER_MINUTE_LOCAL,
        BLEND_CPC_REFRESH_SCHEDULER_TZ,
    )

    while True:
        try:
            delay = _seconds_until_local_time(
                int(BLEND_CPC_REFRESH_SCHEDULER_HOUR_LOCAL),
                int(BLEND_CPC_REFRESH_SCHEDULER_MINUTE_LOCAL),
                BLEND_CPC_REFRESH_SCHEDULER_TZ,
            )
            logger.info(
                "Blend CPC refresh scheduler: sleeping %.0fs until %02d:%02d %s",
                delay,
                int(BLEND_CPC_REFRESH_SCHEDULER_HOUR_LOCAL),
                int(BLEND_CPC_REFRESH_SCHEDULER_MINUTE_LOCAL),
                BLEND_CPC_REFRESH_SCHEDULER_TZ,
            )
            time.sleep(delay)
            _run_once()
        except Exception:
            logger.exception("Blend CPC refresh scheduler run failed")
            time.sleep(60)


def start_blend_cpc_refresh_scheduler() -> None:
    global _started
    from config import (
        BLEND_CPC_REFRESH_SCHEDULER_ENABLED,
        BLEND_CPC_REFRESH_SCHEDULER_HOUR_LOCAL,
        BLEND_CPC_REFRESH_SCHEDULER_MINUTE_LOCAL,
        BLEND_CPC_REFRESH_SCHEDULER_TZ,
    )

    if not BLEND_CPC_REFRESH_SCHEDULER_ENABLED:
        logger.info("Blend CPC refresh scheduler disabled (BLEND_CPC_REFRESH_SCHEDULER_ENABLED)")
        return
    if os.getenv("FLASK_DEBUG") == "1" and os.environ.get("WERKZEUG_RUN_MAIN") == "false":
        return
    if _started:
        return
    threading.Thread(target=_loop, name="blend-cpc-refresh-scheduler", daemon=True).start()
    _started = True
    logger.info(
        "Blend CPC refresh scheduler started (daily %02d:%02d %s)",
        int(BLEND_CPC_REFRESH_SCHEDULER_HOUR_LOCAL),
        int(BLEND_CPC_REFRESH_SCHEDULER_MINUTE_LOCAL),
        BLEND_CPC_REFRESH_SCHEDULER_TZ,
    )
