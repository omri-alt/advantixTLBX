from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from integrations.autoserver import skunmon as skunmon

logger = logging.getLogger(__name__)


class PauseUnmonSK(BaseAutomation):
    """Hourly: pause SK campaigns when Kelkoo HP is unmonetized."""

    def on_hourly_signal(self, hour: int) -> None:
        if hour in list(range(24)):
            logger.info("PauseUnmonSK hourly at hour %s", hour)
            self._wrap_run("scheduler", self._execute)

    def run_manually(self) -> dict[str, Any]:
        logger.info("PauseUnmonSK manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        logger.info("Executing PauseUnmonSK")
        wait_s = 2.0
        for attempt in range(3):
            try:
                skunmon.pause_Unmonetized_KL()
                return
            except Exception as e:
                msg = str(e)
                transient = (
                    "429" in msg
                    or "Quota exceeded" in msg
                    or "Rate Limit" in msg
                    or "timeout" in msg.lower()
                )
                if transient and attempt < 2:
                    logger.warning(
                        "PauseUnmonSK transient failure (attempt %s/3): %s",
                        attempt + 1,
                        e,
                    )
                    time.sleep(wait_s)
                    wait_s *= 2.0
                    continue
                # Avoid flipping the whole scheduler run red on one fragile SK row/parsing issue.
                logger.error("PauseUnmonSK completed with non-fatal error: %s", e)
                return
