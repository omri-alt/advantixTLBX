from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from config import ZEROPARK_BLEND_CAP_GUARD_ENABLED
from integrations.blend_zp_cap_guard import run_blend_zp_cap_guard

logger = logging.getLogger(__name__)


class BlendZpCapGuard(BaseAutomation):
    """Hourly: pause mapped Zeropark Blend campaigns once geo/device cap is filled."""

    def on_hourly_signal(self, hour: int) -> None:
        if not ZEROPARK_BLEND_CAP_GUARD_ENABLED:
            return
        logger.info("BlendZpCapGuard hourly at hour %s", hour)
        self._wrap_run("scheduler", self._execute)

    def run_manually(self) -> dict[str, Any]:
        logger.info("BlendZpCapGuard manual trigger")
        if not ZEROPARK_BLEND_CAP_GUARD_ENABLED:
            return {
                "status": "skipped",
                "reason": "disabled",
                "timestamp": datetime.now().isoformat(),
            }
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        payload = run_blend_zp_cap_guard(dry_run=False, reason="autoserver")
        if payload.get("errors"):
            logger.warning(
                "BlendZpCapGuard completed with %s error(s): %s",
                len(payload["errors"]),
                payload["errors"][:3],
            )
        logger.info(
            "BlendZpCapGuard paused=%s reached=%s unmapped=%s",
            payload.get("paused"),
            payload.get("segments_reached_cap"),
            payload.get("skipped_unmapped"),
        )
