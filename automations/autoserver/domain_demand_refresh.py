from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from config import DOMAIN_DEMAND_ENABLED, DOMAIN_TRILLION_GUARD_ENABLED
from integrations.domain_demand import today_domain_demand_ready
from integrations.domain_demand_guard import run_domain_demand_guard

logger = logging.getLogger(__name__)


class DomainDemandRefresh(BaseAutomation):
    """Refresh domain-demand sheet, equalize flow weights, pause filled Trillion segments.

    After nightly rollover the live bill is empty until morning daily rebuild — this job
    then only no-ops (no rebuild from yesterday's Nipuhim/Blend, no Trillion resume).
    """

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
        if not today_domain_demand_ready():
            logger.info(
                "DomainDemandRefresh: live bill empty (awaiting morning daily) — skip rebuild/resume"
            )
            return {
                "status": "awaiting_morning",
                "trillion_resumed": 0,
                "trillion_paused": 0,
                "hub_streams_updated": 0,
                "total_demand": 0,
                "errors": [],
            }

        # Mid-day: rebuild from live clickCaps so media-buyer bumps feed the bill + weights.
        # Never resume here — only pause overfilled; morning 7g handles activate.
        result = run_domain_demand_guard(
            rebuild_demand=True,
            dry_run=False,
            reason="automation",
            pause_trillion=DOMAIN_TRILLION_GUARD_ENABLED,
            resume_trillion=False,
            equalize_weights=DOMAIN_TRILLION_GUARD_ENABLED,
        )
        pause = result.get("trillion_pause") or {}
        resume = result.get("trillion_resume") or {}
        hub_eq = result.get("hub_equalize") or {}
        return {
            "status": result.get("status", "error"),
            "trillion_resumed": resume.get("resumed"),
            "trillion_paused": pause.get("paused"),
            "hub_streams_updated": hub_eq.get("hub_streams_updated"),
            "total_demand": (result.get("sync") or {}).get("bill_rows"),
            "errors": list(resume.get("errors") or []) + list(pause.get("errors") or []),
            "fallback": hub_eq.get("fallback"),
        }
