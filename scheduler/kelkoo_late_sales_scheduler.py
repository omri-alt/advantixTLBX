from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
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
    from config import (
        KELKOO_LATE_SALES_APPLY_ENABLED,
        KELKOO_LATE_SALES_SPREADSHEET_ID,
        LATE_CONVERSION_SCHEDULER_HOUR_LOCAL,
        LATE_CONVERSION_SCHEDULER_TZ,
        LATE_SALES_POSTBACK_BASE,
    )
    from kelkoo_late_sales import run_late_sales_flow

    sid = (KELKOO_LATE_SALES_SPREADSHEET_ID or "").strip()
    if not sid:
        logger.warning("Late conversion scheduler: KELKOO_LATE_SALES_SPREADSHEET_ID is empty")
        return
    cred = Path(__file__).resolve().parents[1] / "credentials.json"
    if not cred.is_file():
        logger.warning("Late conversion scheduler: credentials.json missing at %s", cred)
        return

    apply = bool(KELKOO_LATE_SALES_APPLY_ENABLED)
    ls = run_late_sales_flow(
        credentials_path=cred,
        spreadsheet_id=sid,
        postback_base=LATE_SALES_POSTBACK_BASE,
        as_of_str="",
        apply=apply,
        refresh_sheets=True,
        prune_tabs=True,
    )
    if ls.get("ok"):
        logger.info(
            "Late conversion run done: window=%s eligible=%s sent_ok=%s sent_fail=%s skipped_keitaro=%s",
            ls.get("sale_window"),
            ls.get("eligible_count"),
            ls.get("postbacks_ok"),
            ls.get("postbacks_fail"),
            ls.get("skipped_keitaro"),
        )
    else:
        logger.error("Late conversion run failed: %s", ls.get("error"))


def _loop() -> None:
    from config import LATE_CONVERSION_SCHEDULER_HOUR_LOCAL, LATE_CONVERSION_SCHEDULER_TZ

    while True:
        try:
            delay = _seconds_until_local_hour(
                int(LATE_CONVERSION_SCHEDULER_HOUR_LOCAL),
                LATE_CONVERSION_SCHEDULER_TZ,
            )
            logger.info(
                "Late conversion scheduler: sleeping %.0fs until %02d:00 %s",
                delay,
                int(LATE_CONVERSION_SCHEDULER_HOUR_LOCAL),
                LATE_CONVERSION_SCHEDULER_TZ,
            )
            time.sleep(delay)
            _run_once()
        except Exception:
            logger.exception("Late conversion scheduler run failed")
            time.sleep(60)


def start_kelkoo_late_sales_scheduler() -> None:
    global _started
    from config import (
        KELKOO_LATE_SALES_APPLY_ENABLED,
        KELKOO_LATE_SALES_SCHEDULER_ENABLED,
        LATE_CONVERSION_SCHEDULER_HOUR_LOCAL,
        LATE_CONVERSION_SCHEDULER_TZ,
    )

    if not KELKOO_LATE_SALES_SCHEDULER_ENABLED:
        logger.info("Late conversion scheduler disabled (KELKOO_LATE_SALES_SCHEDULER_ENABLED)")
        return
    if os.getenv("FLASK_DEBUG") == "1" and os.environ.get("WERKZEUG_RUN_MAIN") == "false":
        return
    if _started:
        return
    threading.Thread(target=_loop, name="late-conversion-scheduler", daemon=True).start()
    _started = True
    logger.info(
        "Late conversion scheduler started (daily %02d:00 %s; apply=%s)",
        int(LATE_CONVERSION_SCHEDULER_HOUR_LOCAL),
        LATE_CONVERSION_SCHEDULER_TZ,
        "on" if KELKOO_LATE_SALES_APPLY_ENABLED else "off",
    )
