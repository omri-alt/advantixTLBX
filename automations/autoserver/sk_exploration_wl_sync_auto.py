from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from integrations.autoserver.sk_exploration_wl_sync import (
    mark_wl_sync_done,
    sync_exploration_wl_from_keitaro_sales,
)

logger = logging.getLogger(__name__)


def _enabled() -> bool:
    v = (os.getenv("SK_EXPLORATION_WL_SYNC_ENABLED") or "yes").strip().lower()
    return v not in ("0", "false", "no", "off")


class SKExplorationWlSyncAuto(BaseAutomation):
    """
    Daily: append SK publisher subIds to ``SKtrackExploration.wl`` from Keitaro
    ``SaleOur`` / ``LateSale`` conversions (30-day lookback).

    Scheduled via APScheduler cron at ``SK_EXPLORATION_WL_SYNC_*`` (default 12:00 Asia/Jerusalem).
    """

    def on_hourly_signal(self, hour: int) -> None:
        pass

    def run_manually(self) -> dict[str, Any]:
        logger.info("SKExplorationWlSyncAuto manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        if not _enabled():
            logger.info("SKExplorationWlSyncAuto skipped (SK_EXPLORATION_WL_SYNC_ENABLED=0)")
            return
        result = sync_exploration_wl_from_keitaro_sales(dry_run=False)
        mark_wl_sync_done()
        qw = result.get("quality_wl") or {}
        if (
            int(result.get("sources_appended") or 0) == 0
            and int(result.get("campaigns_updated") or 0) == 0
            and int(qw.get("appended") or 0) == 0
        ):
            logger.info("SKExplorationWlSyncAuto: no WL changes needed")
        else:
            logger.info(
                "SKExplorationWlSyncAuto: updated %s campaign(s), appended %s source(s), "
                "QualityWL +%s row(s)",
                result.get("campaigns_updated"),
                result.get("sources_appended"),
                qw.get("appended"),
            )
