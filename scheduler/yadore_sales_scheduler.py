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
    import requests

    from config import DAILY_CONVERSION_POSTBACK_STATE_PATH
    from integrations.daily_conversion_postbacks import (
        default_report_date_str,
        run_yadore_conversion_sale_postbacks,
    )

    report_date = default_report_date_str()
    state_path = Path(DAILY_CONVERSION_POSTBACK_STATE_PATH)
    session = requests.Session()
    out = run_yadore_conversion_sale_postbacks(
        report_date,
        state_path=state_path,
        dry_run=False,
        no_resume=False,
        session=session,
    )
    if out.get("ok"):
        logger.info(
            "Yadore daily sales postbacks done: date=%s sales=%s sent=%s failed=%s",
            report_date,
            out.get("sales"),
            out.get("sent"),
            out.get("failed", 0),
        )
        try:
            from integrations.daily_postbacks_run_history import record_last_run

            record_last_run(
                "yadore_sales",
                report_date,
                dry_run=False,
                ok=True,
                summary=out,
                batch_exit_code=0,
            )
        except Exception:
            logger.exception("Yadore sales scheduler: could not write last-run history")
    else:
        logger.error("Yadore daily sales postbacks failed: %s", out.get("error", out))


def _loop() -> None:
    from config import YADORE_SALES_SCHEDULER_HOUR_LOCAL, YADORE_SALES_SCHEDULER_TZ

    while True:
        try:
            delay = _seconds_until_local_hour(
                int(YADORE_SALES_SCHEDULER_HOUR_LOCAL),
                YADORE_SALES_SCHEDULER_TZ,
            )
            logger.info(
                "Yadore sales scheduler: sleeping %.0fs until %02d:00 %s",
                delay,
                int(YADORE_SALES_SCHEDULER_HOUR_LOCAL),
                YADORE_SALES_SCHEDULER_TZ,
            )
            time.sleep(delay)
            _run_once()
        except Exception:
            logger.exception("Yadore sales scheduler run failed")
            time.sleep(60)


def start_yadore_sales_scheduler() -> None:
    global _started
    from config import YADORE_SALES_SCHEDULER_ENABLED, YADORE_SALES_SCHEDULER_HOUR_LOCAL, YADORE_SALES_SCHEDULER_TZ

    if not YADORE_SALES_SCHEDULER_ENABLED:
        logger.info("Yadore sales scheduler disabled (YADORE_SALES_SCHEDULER_ENABLED=0)")
        return
    if os.getenv("FLASK_DEBUG") == "1" and os.environ.get("WERKZEUG_RUN_MAIN") == "false":
        return
    if _started:
        return
    threading.Thread(target=_loop, name="yadore-sales-scheduler", daemon=True).start()
    _started = True
    logger.info(
        "Yadore sales scheduler started (daily %02d:00 %s, yesterday SaleOur)",
        int(YADORE_SALES_SCHEDULER_HOUR_LOCAL),
        YADORE_SALES_SCHEDULER_TZ,
    )
