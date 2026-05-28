from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation
from config import TRILLION_BLEND_CAP_GUARD_ENABLED
from integrations.blend_tr_cap_guard import run_blend_tr_cap_guard

logger = logging.getLogger(__name__)


class BlendTrCapGuard(BaseAutomation):
    """Every N minutes: pause over-cap Trillion Blend campaigns."""

    def on_hourly_signal(self, hour: int) -> None:
        if not TRILLION_BLEND_CAP_GUARD_ENABLED:
            return
        logger.info("BlendTrCapGuard scheduler tick at hour %s", hour)
        self._wrap_run("scheduler", self._execute_pause_over_cap)

    def run_manually(self) -> dict[str, Any]:
        logger.info("BlendTrCapGuard manual trigger: pause_over_cap")
        if not TRILLION_BLEND_CAP_GUARD_ENABLED:
            return {
                "status": "skipped",
                "reason": "disabled",
                "timestamp": datetime.now().isoformat(),
            }
        out = self._wrap_run("manual", self._execute_pause_over_cap)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def run_manual_action(self, action: str) -> dict[str, Any]:
        logger.info("BlendTrCapGuard manual action: %s", action)
        if not TRILLION_BLEND_CAP_GUARD_ENABLED:
            return {
                "status": "skipped",
                "reason": "disabled",
                "timestamp": datetime.now().isoformat(),
            }
        action_key = (action or "").strip().lower()
        if action_key == "resume_under_cap":
            out = self._wrap_run(
                "manual:resume_under_cap",
                self._execute_resume_under_cap,
            )
            out["timestamp"] = datetime.now().isoformat()
            return out
        raise ValueError(f"Unsupported BlendTrCapGuard action: {action}")

    def _log_payload(self, payload: dict[str, Any]) -> None:
        if payload.get("errors"):
            logger.warning(
                "BlendTrCapGuard completed with %s error(s): %s",
                len(payload["errors"]),
                payload["errors"][:3],
            )
        logger.info(
            "BlendTrCapGuard mode=%s performed=%s matched=%s unmapped=%s",
            payload.get("mode"),
            payload.get("performed"),
            payload.get("segments_matched"),
            payload.get("skipped_unmapped"),
        )

    def _execute_pause_over_cap(self) -> None:
        payload = run_blend_tr_cap_guard(
            dry_run=False,
            reason="autoserver",
            mode="pause_over_cap",
        )
        self._log_payload(payload)

    def _execute_resume_under_cap(self) -> None:
        payload = run_blend_tr_cap_guard(
            dry_run=False,
            reason="manual_resume_under_cap",
            mode="resume_under_cap",
        )
        self._log_payload(payload)
