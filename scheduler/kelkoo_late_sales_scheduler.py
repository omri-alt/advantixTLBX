from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_started = False


def _seconds_until_hour_utc(hour_utc: int) -> float:
    now = datetime.now(timezone.utc)
    target = now.replace(hour=hour_utc, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return max(1.0, (target - now).total_seconds())


def _run_once() -> None:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    from config import KELKOO_LATE_SALES_SPREADSHEET_ID
    from workflows.kelkoo_sales_report import run_yesterday_sales_reports

    sid = (KELKOO_LATE_SALES_SPREADSHEET_ID or "").strip()
    if not sid:
        logger.warning("Kelkoo late-sales prep scheduler: KELKOO_LATE_SALES_SPREADSHEET_ID is empty")
        return
    cred = Path(__file__).resolve().parents[1] / "credentials.json"
    if not cred.is_file():
        logger.warning("Kelkoo late-sales prep scheduler: credentials.json missing at %s", cred)
        return
    creds = service_account.Credentials.from_service_account_file(str(cred))
    service = build("sheets", "v4", credentials=creds).spreadsheets()
    res = run_yesterday_sales_reports(service, dry_run=False)
    seven = res.get("seven_day") if isinstance(res, dict) else {}
    logger.info(
        "Kelkoo late-sales prep done: daily_tabs=%s seven_day_tab=%s seven_day_rows=%s",
        len((res.get("tabs") or []) if isinstance(res, dict) else []),
        (seven or {}).get("tab"),
        (seven or {}).get("rows"),
    )


def _loop() -> None:
    from config import KELKOO_LATE_SALES_SCHEDULER_HOUR_UTC

    while True:
        try:
            delay = _seconds_until_hour_utc(int(KELKOO_LATE_SALES_SCHEDULER_HOUR_UTC))
            logger.info("Kelkoo late-sales prep scheduler: sleeping %.0fs", delay)
            time.sleep(delay)
            _run_once()
        except Exception:
            logger.exception("Kelkoo late-sales prep scheduler run failed")
            time.sleep(60)


def start_kelkoo_late_sales_scheduler() -> None:
    global _started
    from config import (
        KELKOO_LATE_SALES_SCHEDULER_ENABLED,
        KELKOO_LATE_SALES_SCHEDULER_HOUR_UTC,
    )

    if not KELKOO_LATE_SALES_SCHEDULER_ENABLED:
        logger.info("Kelkoo late-sales prep scheduler disabled (KELKOO_LATE_SALES_SCHEDULER_ENABLED)")
        return
    # Skip only Werkzeug reloader parent process.
    if os.getenv("FLASK_DEBUG") == "1" and os.environ.get("WERKZEUG_RUN_MAIN") == "false":
        return
    if _started:
        return
    threading.Thread(target=_loop, name="kelkoo-late-sales-prep-scheduler", daemon=True).start()
    _started = True
    logger.info(
        "Kelkoo late-sales prep scheduler started (daily at %02d:00 UTC)",
        int(KELKOO_LATE_SALES_SCHEDULER_HOUR_UTC),
    )
