from __future__ import annotations

import logging
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
        sk_optimizer.checkUnmonExploration_SK()
        sk_optimizer.checkUnmonWL_SK()
