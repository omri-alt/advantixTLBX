from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from config import ZEROPARK_BLEND_NIGHTLY_CLOSE_ENABLED
from integrations.blend_zp_cap_guard import pause_all_mapped_blend_zp_campaigns

logger = logging.getLogger(__name__)


class CloseBlendZpAuto(BaseAutomation):
    """
    Daily Zeropark close: pause all mapped Blend campaigns on ``ZP BLEND campaignsID``.

    Scheduled via APScheduler cron (``ZEROPARK_BLEND_CLOSE_*``, defaults to Nipuhim close time).
    """

    def on_hourly_signal(self, hour: int) -> None:
        pass

    def run_manually(self) -> dict[str, Any]:
        logger.info("CloseBlendZpAuto manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        if not ZEROPARK_BLEND_NIGHTLY_CLOSE_ENABLED:
            logger.info("CloseBlendZpAuto skipped (ZEROPARK_BLEND_NIGHTLY_CLOSE_ENABLED=0)")
            return
        logger.info("Executing CloseBlendZpAuto")
        payload = pause_all_mapped_blend_zp_campaigns(dry_run=False, reason="autoserver")
        if payload.get("errors"):
            logger.warning(
                "CloseBlendZpAuto completed with %s error(s): %s",
                len(payload["errors"]),
                payload["errors"][:3],
            )
