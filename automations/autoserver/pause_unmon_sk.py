from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from integrations.autoserver import skunmon as skunmon

logger = logging.getLogger(__name__)


class PauseUnmonSK(BaseAutomation):
    """Hourly: pause SK campaigns when Kelkoo HP is unmonetized."""

    def on_hourly_signal(self, hour: int) -> None:
        if hour in list(range(24)):
            logger.info("PauseUnmonSK hourly at hour %s", hour)
            self._wrap_run("scheduler", self._execute)

    def run_manually(self) -> dict[str, Any]:
        logger.info("PauseUnmonSK manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        logger.info("Executing PauseUnmonSK")
        skunmon.pause_Unmonetized_KL()
