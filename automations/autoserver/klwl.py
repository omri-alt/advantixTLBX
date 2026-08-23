from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from integrations.autoserver import sk as sk

logger = logging.getLogger(__name__)


class KLWL(BaseAutomation):
    """Every even hour: SK KLWL source list + optimize."""

    def on_hourly_signal(self, hour: int) -> None:
        if hour in list(range(24)) and hour % 2 == 0:
            logger.info("KLWL hourly at hour %s", hour)
            self._wrap_run("scheduler", self._execute)

    def run_manually(self) -> dict[str, Any]:
        logger.info("KLWL manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        logger.info("Executing KLWL")
        sourcim = [
            "sb1a0e6971aea25a",  # KLWL11
        ]
        sk.findSourceListInCampaigns(sourcim)
        sk.optimize_KLWL1()
