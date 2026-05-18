from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from integrations.autoserver import zp as zp

logger = logging.getLogger(__name__)


class CloseNipuhimAuto(BaseAutomation):
    """
    Daily Zeropark close: pause all ``generalMehila-*`` campaigns.

    Scheduled via APScheduler cron (``ZEROPARK_CLOSE_HOUR`` / ``MINUTE`` in ``ZEROPARK_CLOSE_TZ``),
    not the legacy server-local hour-23 hourly gate.
    """

    def on_hourly_signal(self, hour: int) -> None:
        # Fired by ``scheduler/autoserver_scheduler.py`` cron at configured Zeropark panel time.
        pass

    def run_manually(self) -> dict[str, Any]:
        logger.info("CloseNipuhimAuto manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        logger.info("Executing CloseNipuhimAuto")
        zp.pause_generalMehila()
