from __future__ import annotations

import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from automations.autoserver.base_automation import BaseAutomation
from config import (
    BLEND_HUB_V2_ENABLED,
    BLEND_SYNC_QUIET_END_HOUR,
    BLEND_SYNC_QUIET_START_HOUR,
    BLEND_SYNC_QUIET_TZ,
    DOMAIN_DEMAND_ENABLED,
    DOMAIN_TRILLION_GUARD_ENABLED,
)

logger = logging.getLogger(__name__)


def _in_blend_sync_quiet_hours(now: datetime | None = None) -> bool:
    """True during [start, end) local hours (wraps midnight if start > end)."""
    try:
        tz = ZoneInfo(BLEND_SYNC_QUIET_TZ or "Asia/Jerusalem")
    except Exception:
        tz = ZoneInfo("Asia/Jerusalem")
    local = (now or datetime.now(tz)).astimezone(tz)
    hour = local.hour
    start = int(BLEND_SYNC_QUIET_START_HOUR)
    end = int(BLEND_SYNC_QUIET_END_HOUR)
    if start == end:
        return False
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end


class BlendSync2h(BaseAutomation):
    """Every 2 hours: re-sync Blend sheet → Keitaro, then rebuild domain demand + weights.

    Runs ``blend_sync_from_sheet.py`` which:
      - Detaches Blend offers not monetized in the current potential snapshot.
      - Re-checks Kelkoo monetization on auto='v' sheet rows.
      - Refreshes device weights on the sheet when clickCap/CPC no longer match.
      - Re-applies clickCap-weighted shares per geo desktop/mobile flow (legacy Blend campaign).

    Then (when enabled):
      - Syncs BLEND-feed* hub child campaigns (blend v2).
      - Rebuilds the domain-demand bill from live clickCaps and equalizes hub 94 /
        BLEND-feed* / Quality domain weights; pauses filled Trillion segments.

    Cadence: even hours (0, 2, 4, ..., 22) on the existing hourly scheduler tick.
    Quiet window (default Asia/Jerusalem 01:00–10:00): no scheduled runs after
    Trillion nightly close until morning.
    """

    def on_hourly_signal(self, hour: int) -> None:
        if hour % 2 != 0:
            return
        if _in_blend_sync_quiet_hours():
            logger.info(
                "BlendSync2h skipped (quiet hours %s-%s %s); scheduler hour=%s",
                BLEND_SYNC_QUIET_START_HOUR,
                BLEND_SYNC_QUIET_END_HOUR,
                BLEND_SYNC_QUIET_TZ,
                hour,
            )
            return
        logger.info("BlendSync2h triggered at hour %s", hour)
        self._wrap_run("scheduler", self._execute)

    def run_manually(self) -> dict[str, Any]:
        logger.info("BlendSync2h manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> dict[str, Any]:
        repo_root = Path(__file__).resolve().parents[2]
        legacy = self._run_script(repo_root, "blend_sync_from_sheet.py")

        blend_v2: dict[str, Any] = {"status": "skipped", "reason": "disabled"}
        if BLEND_HUB_V2_ENABLED:
            blend_v2 = self._run_script(repo_root, "blend_sync_from_sheet_v2.py")

        demand: dict[str, Any] = {"status": "skipped", "reason": "disabled"}
        if DOMAIN_DEMAND_ENABLED:
            from integrations.domain_demand import today_domain_demand_ready
            from integrations.domain_demand_guard import run_domain_demand_guard

            if not today_domain_demand_ready():
                demand = {
                    "status": "awaiting_morning",
                    "sync": {"bill_rows": 0},
                    "trillion_pause": {"paused": 0},
                    "hub_equalize": {"hub_streams_updated": 0},
                }
                logger.info("BlendSync2h: domain-demand bill empty — skip overnight rebuild/resume")
            else:
                demand = run_domain_demand_guard(
                    rebuild_demand=True,
                    dry_run=False,
                    reason="blend_sync_2h",
                    pause_trillion=DOMAIN_TRILLION_GUARD_ENABLED,
                    resume_trillion=False,
                    equalize_weights=DOMAIN_TRILLION_GUARD_ENABLED,
                )
                logger.info(
                    "BlendSync2h domain demand: status=%s bill_rows=%s hub_updated=%s paused=%s",
                    demand.get("status"),
                    (demand.get("sync") or {}).get("bill_rows"),
                    (demand.get("hub_equalize") or {}).get("hub_streams_updated"),
                    (demand.get("trillion_pause") or {}).get("paused"),
                )

        return {
            "legacy_blend_sync": legacy,
            "blend_v2_sync": blend_v2,
            "domain_demand": {
                "status": demand.get("status"),
                "bill_rows": (demand.get("sync") or {}).get("bill_rows"),
                "hub_streams_updated": (demand.get("hub_equalize") or {}).get(
                    "hub_streams_updated"
                ),
                "trillion_paused": (demand.get("trillion_pause") or {}).get("paused"),
                "error": demand.get("error"),
            },
        }

    def _run_script(self, repo_root: Path, script_name: str) -> dict[str, Any]:
        script = repo_root / script_name
        cmd = [sys.executable, str(script)]
        logger.info("BlendSync2h: running %s", " ".join(cmd))
        result = subprocess.run(cmd, cwd=str(repo_root))
        if result.returncode != 0:
            raise RuntimeError(f"{script_name} exited with code {result.returncode}")
        return {"status": "ok", "script": script_name}
