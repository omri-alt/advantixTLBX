from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from integrations.autoserver import zp as zp

logger = logging.getLogger(__name__)


class CloseNipuhimAuto(BaseAutomation):
    """Hour 23 only: Zeropark close nipuhim (general mehila pause)."""

    def on_hourly_signal(self, hour: int) -> None:
        if hour in (23,):
            logger.info("CloseNipuhimAuto at hour %s", hour)
            self._wrap_run("scheduler", self._execute)

    def run_manually(self) -> dict[str, Any]:
        logger.info("CloseNipuhimAuto manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        logger.info("Executing CloseNipuhimAuto")
        zp.pause_generalMehila()
