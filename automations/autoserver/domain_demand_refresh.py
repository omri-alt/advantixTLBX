from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from config import DOMAIN_DEMAND_ENABLED, DOMAIN_TRILLION_GUARD_ENABLED
from integrations.domain_demand_guard import run_domain_demand_guard

logger = logging.getLogger(__name__)


class DomainDemandRefresh(BaseAutomation):
    """Refresh domain-demand sheet, equalize flow weights, pause filled Trillion segments."""

    def on_hourly_signal(self, hour: int) -> None:
        if not DOMAIN_DEMAND_ENABLED:
            return
        logger.info("DomainDemandRefresh scheduler tick at hour %s", hour)
        self._wrap_run("scheduler", self._execute)

    def run_manually(self) -> dict[str, Any]:
        if not DOMAIN_DEMAND_ENABLED:
            return {
                "status": "skipped",
                "reason": "disabled",
                "timestamp": datetime.now().isoformat(),
            }
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> dict[str, Any]:
        # Always rebuild demand from live Blend clickCaps + Nipuhim offers so
        # media-buyer mid-day clickCap bumps feed into the bill and hub weights.
        result = run_domain_demand_guard(
            rebuild_demand=True,
            dry_run=False,
            reason="automation",
            pause_trillion=DOMAIN_TRILLION_GUARD_ENABLED,
            equalize_weights=DOMAIN_TRILLION_GUARD_ENABLED,
        )
        pause = result.get("trillion_pause") or {}
        hub_eq = result.get("hub_equalize") or {}
        return {
            "status": result.get("status", "error"),
            "trillion_paused": pause.get("paused"),
            "hub_streams_updated": hub_eq.get("hub_streams_updated"),
            "total_demand": (result.get("sync") or {}).get("bill_rows"),
            "errors": pause.get("errors") or [],
            "fallback": hub_eq.get("fallback"),
        }
