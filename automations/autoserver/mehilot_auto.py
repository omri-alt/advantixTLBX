from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from integrations.autoserver import ec as ec
from integrations.autoserver import zp as zp

logger = logging.getLogger(__name__)


class MehilotAuto(BaseAutomation):
    """Hourly: Zeropark mehilot + Ecomnia track sheets."""

    def on_hourly_signal(self, hour: int) -> None:
        if hour in list(range(24)):
            logger.info("MehilotAuto hourly at hour %s", hour)
            self._wrap_run("scheduler", self._execute)

    def run_manually(self) -> dict[str, Any]:
        logger.info("MehilotAuto manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        logger.info("Executing MehilotAuto (Zeropark + Ecomnia)")
        zp.mehilot()
        zp.pause_generalMehila2k()
        zp.generalMehilaMon()
        ec.update_track_sheet()
        ec.checkUnmonExploration()
        ec.update_trackWLsheet()
