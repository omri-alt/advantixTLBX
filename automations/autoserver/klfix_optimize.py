from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from integrations.autoserver import sk as sk

logger = logging.getLogger(__name__)


class KLFIXoptimize(BaseAutomation):
    """Hourly: SK KLFIX new-source optimization."""

    def on_hourly_signal(self, hour: int) -> None:
        if hour in list(range(24)):
            logger.info("KLFIXoptimize hourly at hour %s", hour)
            self._wrap_run("scheduler", self._execute)

    def run_manually(self) -> dict[str, Any]:
        logger.info("KLFIXoptimize manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        logger.info("Executing KLFIXoptimize")
        sk.get_activeCampaigns()
        sk.optimize_newsource_fix7days2()
