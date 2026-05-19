"""
Start in-process background schedulers (AutoServer, Kelkoo late-sales prep, overview).

Under Gunicorn, only the worker selected in ``gunicorn.conf.py`` should call this
(see ``KLBLEND_SCHEDULER_WORKER=1``). Flask dev / ``python app.py`` start at import.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def should_start_background_schedulers_at_import() -> bool:
    """False when Gunicorn defers startup to ``post_fork`` (one worker only)."""
    if os.getenv("KLBLEND_DEFER_SCHEDULERS", "").strip().lower() in ("1", "true", "yes"):
        return False
    return True


def is_designated_scheduler_worker() -> bool:
    """True when this process is allowed to run background schedulers."""
    flag = os.getenv("KLBLEND_SCHEDULER_WORKER", "").strip()
    if flag == "1":
        return True
    if flag == "0":
        return False
    # Not under Gunicorn defer mode: any process that loads the app may run schedulers.
    return should_start_background_schedulers_at_import()


def start_background_schedulers() -> None:
    """Idempotent-ish: each ``start_*`` guards with module-level flags."""
    if not is_designated_scheduler_worker():
        logger.info(
            "Background schedulers skipped on worker pid=%s (not designated scheduler worker)",
            os.getpid(),
        )
        return

    from scheduler.autoserver_scheduler import start_autoserver_scheduler
    from scheduler.kelkoo_late_sales_scheduler import start_kelkoo_late_sales_scheduler
    from integrations.overview_snapshot import (
        start_daily_overview_scheduler,
        start_overview_snapshot_bootstrap,
    )

    for label, fn in (
        ("AutoServer", start_autoserver_scheduler),
        ("Kelkoo late-sales prep", start_kelkoo_late_sales_scheduler),
        ("Overview snapshot", start_daily_overview_scheduler),
        ("Overview bootstrap", start_overview_snapshot_bootstrap),
    ):
        try:
            fn()
        except Exception as e:
            logger.warning("%s scheduler did not start: %s", label, e)
