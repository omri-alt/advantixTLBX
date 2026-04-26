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
            "s3ed3a7177c013e2",
            "sfb01bfc6ac1cbe3",
            "s6edc9136846d915",
            "s27b58e2b6548902",
            "s06bc48fe7a74470",
            "s2599879d2841979",
            "s1bf84bf08ddb9e4",
            "sab80d384b9e33bf",
            "s329c543a1b75b0b",
        ]
        sk.findSourceListInCampaigns(sourcim)
        sk.optimize_KLWL1()
