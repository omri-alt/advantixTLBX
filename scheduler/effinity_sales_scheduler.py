from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_started = False


def _seconds_until_local_hour(hour_local: int, tz_name: str) -> float:
    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)
    target = now.replace(hour=hour_local, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return max(1.0, (target - now).total_seconds())


def _run_once() -> None:
    from integrations.daily_conversion_postbacks import default_report_date_str
    from late_conversion_sales import apply_effinity_mtd_cpasale_backlog

    report_date = default_report_date_str()
    out = apply_effinity_mtd_cpasale_backlog(dry_run=False)
    if out.get("ok"):
        logger.info(
            "Effinity daily sales postbacks done: window=%s sales=%s eligible=%s sent_ok=%s sent_fail=%s",
            out.get("sale_window"),
            out.get("effinity_sales_found"),
            out.get("eligible"),
            out.get("postbacks_ok"),
            out.get("postbacks_fail"),
        )
        try:
            from integrations.daily_postbacks_run_history import record_last_run

            record_last_run(
                "effinity",
                report_date,
                dry_run=False,
                ok=True,
                summary=out,
                batch_exit_code=0,
            )
        except Exception:
            logger.exception("Effinity sales scheduler: could not write last-run history")
    else:
        logger.error("Effinity daily sales postbacks failed: %s", out.get("error", out))


def _loop() -> None:
    from config import EFFINITY_SALES_SCHEDULER_HOUR_LOCAL, EFFINITY_SALES_SCHEDULER_TZ

    while True:
        try:
            delay = _seconds_until_local_hour(
                int(EFFINITY_SALES_SCHEDULER_HOUR_LOCAL),
                EFFINITY_SALES_SCHEDULER_TZ,
            )
            logger.info(
                "Effinity sales scheduler: sleeping %.0fs until %02d:00 %s",
                delay,
                int(EFFINITY_SALES_SCHEDULER_HOUR_LOCAL),
                EFFINITY_SALES_SCHEDULER_TZ,
            )
            time.sleep(delay)
            _run_once()
        except Exception:
            logger.exception("Effinity sales scheduler run failed")
            time.sleep(60)


def start_effinity_sales_scheduler() -> None:
    global _started
    from config import (
        EFFINITY_API_KEY,
        EFFINITY_SALES_SCHEDULER_ENABLED,
        EFFINITY_SALES_SCHEDULER_HOUR_LOCAL,
        EFFINITY_SALES_SCHEDULER_TZ,
    )

    if not (EFFINITY_API_KEY or "").strip():
        return
    if not EFFINITY_SALES_SCHEDULER_ENABLED:
        logger.info("Effinity sales scheduler disabled (EFFINITY_SALES_SCHEDULER_ENABLED=0)")
        return
    if os.getenv("FLASK_DEBUG") == "1" and os.environ.get("WERKZEUG_RUN_MAIN") == "false":
        return
    if _started:
        return
    threading.Thread(target=_loop, name="effinity-sales-scheduler", daemon=True).start()
    _started = True
    logger.info(
        "Effinity sales scheduler started (daily %02d:00 %s, MTD salecpa backlog)",
        int(EFFINITY_SALES_SCHEDULER_HOUR_LOCAL),
        EFFINITY_SALES_SCHEDULER_TZ,
    )
