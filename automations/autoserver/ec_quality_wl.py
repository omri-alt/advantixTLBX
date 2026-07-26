from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation

logger = logging.getLogger(__name__)


class ECQualityWL(BaseAutomation):
    """Every even hour: refresh ``ECQualityWL`` clicks / bid / status from EC reports."""

    def on_hourly_signal(self, hour: int) -> None:
        if hour % 2 == 0:
            logger.info("ECQualityWL hourly at hour %s", hour)
            self._wrap_run("scheduler", self._execute)

    def run_manually(self) -> dict[str, Any]:
        logger.info("ECQualityWL manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        from integrations.autoserver.env import ensure_autoserver_env
        from integrations.autoserver.ec_exploration_wl_sync import refresh_ec_quality_wl

        ensure_autoserver_env()
        result = refresh_ec_quality_wl()
        logger.info("ECQualityWL sheet updated: %s", result)
