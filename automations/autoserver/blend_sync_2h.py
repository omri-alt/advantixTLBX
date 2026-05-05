from __future__ import annotations

import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from automations.autoserver.base_automation import BaseAutomation

logger = logging.getLogger(__name__)


class BlendSync2h(BaseAutomation):
    """Every 2 hours: re-sync the Blend sheet to the Keitaro Blend campaign.

    Runs ``blend_sync_from_sheet.py`` which:
      - Prunes/zeroes Blend offers not monetized in the current potential snapshot
        (kept attached at share=0 — operators can re-enable later).
      - Re-checks Kelkoo monetization on auto='v' sheet rows.
      - Re-applies clickCap-weighted shares per geo flow.

    Cadence: even hours (0, 2, 4, ..., 22) on the existing hourly scheduler tick.
    """

    def on_hourly_signal(self, hour: int) -> None:
        if hour % 2 != 0:
            return
        logger.info("BlendSync2h triggered at hour %s", hour)
        self._wrap_run("scheduler", self._execute)

    def run_manually(self) -> dict[str, Any]:
        logger.info("BlendSync2h manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        # Run as a subprocess so the script's argparse / sheet auth flow runs
        # exactly as it does in the daily workflow path.
        repo_root = Path(__file__).resolve().parents[2]
        script = repo_root / "blend_sync_from_sheet.py"
        cmd = [sys.executable, str(script)]
        logger.info("BlendSync2h: running %s", " ".join(cmd))
        result = subprocess.run(cmd, cwd=str(repo_root))
        if result.returncode != 0:
            raise RuntimeError(
                f"blend_sync_from_sheet.py exited with code {result.returncode}"
            )
