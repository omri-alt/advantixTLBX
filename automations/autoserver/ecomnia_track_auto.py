from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from integrations.autoserver import ec as ec

logger = logging.getLogger(__name__)


class EcomniaTrackAuto(BaseAutomation):
    """Hourly: Ecomnia exploration + WL track sheets (decoupled from legacy Mehilot/Zeropark)."""

    def on_hourly_signal(self, hour: int) -> None:
        if hour in list(range(24)):
            logger.info("EcomniaTrackAuto hourly at hour %s", hour)
            self._wrap_run("scheduler", self._execute)

    def run_manually(self) -> dict[str, Any]:
        logger.info("EcomniaTrackAuto manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        logger.info("Executing EcomniaTrackAuto")
        ec.update_track_sheet()
        ec.checkUnmonExploration()
        ec.update_trackWLsheet()
