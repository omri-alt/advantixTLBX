from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

from automations.autoserver.base_automation import BaseAutomation

logger = logging.getLogger(__name__)


def _enabled() -> bool:
    v = (os.getenv("KEITARO_FEED_BALANCE_CHECKMON_ENABLED") or "yes").strip().lower()
    return v not in ("0", "false", "no", "off")


class KeitaroFeedBalanceCheckmon(BaseAutomation):
    """Every 2 hours: refresh feed monetization reports in Keitaro campaign notes.

    Soft launch — notes only (no share rebalancing). When Adexa monetizes via golink
    only, the notes include ``adexa_golink:`` with the Keitaro-ready URL.
    """

    def on_hourly_signal(self, hour: int) -> None:
        if hour % 2 != 0:
            return
        if not _enabled():
            logger.info("KeitaroFeedBalanceCheckmon skipped (KEITARO_FEED_BALANCE_CHECKMON_ENABLED=0)")
            return
        logger.info("KeitaroFeedBalanceCheckmon triggered at hour %s", hour)
        self._wrap_run("scheduler", self._execute)

    def run_manually(self) -> dict[str, Any]:
        logger.info("KeitaroFeedBalanceCheckmon manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now().isoformat()
        return out

    def _execute(self) -> None:
        from integrations.keitaro_feed_balance import run_checkmon_update_notes

        result = run_checkmon_update_notes(dry_run=False)
        logger.info(
            "KeitaroFeedBalanceCheckmon: checked=%s updated=%s skipped_unchanged=%s errors=%s",
            result.get("checked_campaigns"),
            result.get("notes_updated"),
            result.get("notes_skipped_unchanged"),
            result.get("notes_errors"),
        )
        if int(result.get("notes_errors") or 0) > 0:
            raise RuntimeError(
                f"feed balance checkmon: {result.get('notes_errors')} note update error(s)"
            )
