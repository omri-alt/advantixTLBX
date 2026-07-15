from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from config import TRILLION_HUB_NIGHTLY_CLOSE_ENABLED
from integrations.nipuhim_tr_nightly_close import pause_trillion_hub_campaigns

logger = logging.getLogger(__name__)


class CloseNipuhimTrAuto(BaseAutomation):
    """
    Daily Trillion close: pause campaigns whose target URL routes to Keitaro hub 94,
    then archive the live domain-demand bill as yesterday and clear today for morning.

    Scheduled at ``TRILLION_HUB_CLOSE_*`` in ``TRILLION_HUB_CLOSE_TZ`` (default 01:00 Asia/Jerusalem).
    """

    def on_hourly_signal(self, hour: int) -> None:
        pass

    def run_manually(self) -> dict[str, Any]:
        logger.info("CloseNipuhimTrAuto manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        if not TRILLION_HUB_NIGHTLY_CLOSE_ENABLED:
            logger.info("CloseNipuhimTrAuto skipped (TRILLION_HUB_NIGHTLY_CLOSE_ENABLED=0)")
            return
        logger.info("Executing CloseNipuhimTrAuto")
        payload = pause_trillion_hub_campaigns(dry_run=False, reason="autoserver")
        if payload.get("errors"):
            logger.warning(
                "CloseNipuhimTrAuto completed with %s error(s): %s",
                len(payload["errors"]),
                payload["errors"][:3],
            )
