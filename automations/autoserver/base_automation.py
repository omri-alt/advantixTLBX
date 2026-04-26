from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from automations.autoserver.run_log import append_run

logger = logging.getLogger(__name__)


class BaseAutomation(ABC):
    """Base class for AutoServer-derived automations (hourly + manual triggers)."""

    def __init__(self) -> None:
        self.name = self.__class__.__name__
        logger.info("Initialized %s", self.name)

    @abstractmethod
    def on_hourly_signal(self, hour: int) -> None:
        """
        Called every hour by the main scheduler (minute 0).

        Args:
            hour: Current local server hour (0–23), same pattern as legacy AutoServer.
        """
        raise NotImplementedError

    @abstractmethod
    def run_manually(self) -> dict[str, Any]:
        """Called when triggered via API (runs in a background job / thread)."""
        raise NotImplementedError

    def _wrap_run(self, triggered_by: str, fn: Callable[[], None]) -> dict[str, Any]:
        name = self.__class__.__name__
        started = datetime.now(timezone.utc)
        err: Optional[str] = None
        status = "success"
        try:
            fn()
        except Exception as e:
            status = "error"
            err = str(e)
            logger.exception("%s failed (%s)", name, triggered_by)
        finished = datetime.now(timezone.utc)
        append_run(
            automation=name,
            triggered_by=triggered_by,
            started_at=started,
            finished_at=finished,
            status=status,
            error=err,
        )
        return {
            "status": status,
            "error": err,
            "started_at": started.isoformat(),
            "finished_at": finished.isoformat(),
        }
