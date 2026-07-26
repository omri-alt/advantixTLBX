from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from integrations.autoserver.ec_exploration_wl_sync import (
    mark_wl_sync_done,
    sync_ec_exploration_wl_from_keitaro_sales,
)

logger = logging.getLogger(__name__)


def _enabled() -> bool:
    v = (os.getenv("EC_EXPLORATION_WL_SYNC_ENABLED") or "yes").strip().lower()
    return v not in ("0", "false", "no", "off")


class ECExplorationWlSyncAuto(BaseAutomation):
    """
    Daily: append EC source ids to ``trackExploration.wl`` from Keitaro
    ``SaleOur`` / ``LateSale`` conversions (``sub_id_6`` ending ``-EC``).

    Reactivates blacklisted sources via absolute ``cpcbysource`` (not bidFactor).
    Appends ``ECQualityWL`` rows. Cron: ``EC_EXPLORATION_WL_SYNC_*``.
    """

    def on_hourly_signal(self, hour: int) -> None:
        pass

    def run_manually(self) -> dict[str, Any]:
        logger.info("ECExplorationWlSyncAuto manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        if not _enabled():
            logger.info("ECExplorationWlSyncAuto skipped (EC_EXPLORATION_WL_SYNC_ENABLED=0)")
            return
        result = sync_ec_exploration_wl_from_keitaro_sales(dry_run=False)
        mark_wl_sync_done()
        qw = result.get("quality_wl") or {}
        if (
            int(result.get("sources_appended") or 0) == 0
            and int(result.get("campaigns_updated") or 0) == 0
            and int(qw.get("appended") or 0) == 0
        ):
            logger.info("ECExplorationWlSyncAuto: no WL changes needed")
        else:
            logger.info(
                "ECExplorationWlSyncAuto: updated %s campaign(s), appended %s source(s), "
                "reactivated %s, activated %s, ECQualityWL +%s",
                result.get("campaigns_updated"),
                result.get("sources_appended"),
                result.get("sources_reactivated"),
                result.get("campaigns_activated"),
                qw.get("appended"),
            )
