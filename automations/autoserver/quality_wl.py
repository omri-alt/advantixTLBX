from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from integrations.autoserver import gdocs_as as gd
from integrations.autoserver import sk as sk

logger = logging.getLogger(__name__)


class QualityWL(BaseAutomation):
    """Every even hour: refresh QualityWL sheet winrates from SK."""

    def on_hourly_signal(self, hour: int) -> None:
        if hour in list(range(24)) and hour % 2 == 0:
            logger.info("QualityWL hourly at hour %s", hour)
            self._wrap_run("scheduler", self._execute)

    def run_manually(self) -> dict[str, Any]:
        logger.info("QualityWL manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        logger.info("Executing QualityWL")
        sheet = gd.read_sheet("QualityWL")
        for i in range(len(sheet)):
            data = sk.findSourceinCampaign(sheet[i]["SUBID"], sheet[i]["CampaignID"])
            sheet[i]["winrate30"] = data["winrate30"]
            sheet[i]["winrate7"] = data["winrate7"]
            sheet[i]["winrateYest"] = data["winrateYest"]
            sheet[i]["winrateToday"] = data["winrateToday"]
            sheet[i]["bid"] = data["bid"]
            sheet[i]["SKstatus"] = data["SKstatus"]
            sheet[i][
                "url"
            ] = f"https://app.sourceknowledge.com/agency/campaigns/{sheet[i]['CampaignID']}/by-channel"
            sheet[i]["lastUpdate"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        gd.create_or_update_sheet_from_dicts("QualityWL", sheet)
        logger.info("QualityWL sheet updated")
