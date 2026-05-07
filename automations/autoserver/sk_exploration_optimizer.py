from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from integrations.autoserver import sk_optimizer

logger = logging.getLogger(__name__)


class SKExplorationOptimizer(BaseAutomation):
    """Hourly: SK exploration + WL sheets — blacklist (bid 0), monetization pause, budget column."""

    def on_hourly_signal(self, hour: int) -> None:
        if hour in list(range(24)):
            logger.info("SKExplorationOptimizer hourly at hour %s", hour)
            self._wrap_run("scheduler", self._execute)

    def run_manually(self) -> dict[str, Any]:
        logger.info("SKExplorationOptimizer manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        logger.info("Executing SKExplorationOptimizer")
        errors: list[str] = []

        def _run_with_retry(label: str, fn) -> None:
            wait_s = 2.0
            for attempt in range(3):
                try:
                    fn()
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
                            "SKExplorationOptimizer %s transient failure (attempt %s/3): %s",
                            label,
                            attempt + 1,
                            e,
                        )
                        time.sleep(wait_s)
                        wait_s *= 2.0
                        continue
                    logger.error("SKExplorationOptimizer %s failed: %s", label, e)
                    errors.append(f"{label}: {e}")
                    return

        _run_with_retry("exploration", sk_optimizer.checkUnmonExploration_SK)
        _run_with_retry("wl", sk_optimizer.checkUnmonWL_SK)

        if errors:
            # Keep scheduler run green for partial progress; details stay in app logs.
            logger.warning("SKExplorationOptimizer completed with non-fatal errors: %s", " | ".join(errors))
