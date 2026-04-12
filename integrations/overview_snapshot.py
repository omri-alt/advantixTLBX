"""
Persisted overview dashboard data (``/api/overview`` reads from disk; rebuild is expensive).

- **Manual / scheduled rebuild:** ``refresh_overview_snapshot()`` (also ``POST /api/overview/refresh``).
- **Daily fire:** background thread sleeps until next ``OVERVIEW_SNAPSHOT_HOUR`` in ``OVERVIEW_SNAPSHOT_TZ`` (default 08:00 UTC).

For multi-worker deployments, set ``OVERVIEW_SCHEDULER_ENABLED=0`` on all but one worker and use cron + ``python cli/refresh_overview_snapshot.py`` instead.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent


def snapshot_path() -> Path:
    from config import OVERVIEW_SNAPSHOT_PATH

    p = (OVERVIEW_SNAPSHOT_PATH or "").strip()
    if p:
        return Path(p)
    return ROOT / "runtime" / "overview_snapshot.json"


def refresh_overview_snapshot() -> Tuple[Dict[str, Any], str]:
    """Run ``build_overview_json`` and atomically write the snapshot file."""
    from integrations.overview import build_overview_json

    data = build_overview_json()
    saved_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    path = snapshot_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    wrapped = {"saved_utc": saved_utc, "data": data}
    tmp.write_text(json.dumps(wrapped, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)
    return data, saved_utc


def read_snapshot_for_api() -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Return ``(payload, saved_utc)`` for ``GET /api/overview``."""
    path = snapshot_path()
    if not path.exists():
        return None, None
    try:
        wrapped = json.loads(path.read_text(encoding="utf-8"))
        data = wrapped.get("data")
        if not isinstance(data, dict):
            return None, None
        saved = wrapped.get("saved_utc")
        return data, str(saved) if saved else None
    except Exception as e:
        logger.warning("Overview snapshot read failed: %s", e)
        return None, None


def _seconds_until_scheduled_fire() -> float:
    from config import OVERVIEW_SNAPSHOT_HOUR, OVERVIEW_SNAPSHOT_TZ

    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo(OVERVIEW_SNAPSHOT_TZ or "UTC")
    except Exception:
        if (OVERVIEW_SNAPSHOT_TZ or "").upper() not in ("", "UTC"):
            logger.warning("Invalid OVERVIEW_SNAPSHOT_TZ %r; using UTC", OVERVIEW_SNAPSHOT_TZ)
        tz = timezone.utc
    hour = int(OVERVIEW_SNAPSHOT_HOUR)
    hour = max(0, min(23, hour))
    now = datetime.now(tz)
    target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return max(1.0, (target - now).total_seconds())


def _scheduler_loop() -> None:
    while True:
        try:
            delay = _seconds_until_scheduled_fire()
            logger.info("Overview snapshot scheduler: sleeping %.0fs until next run", delay)
            time.sleep(delay)
            _, saved = refresh_overview_snapshot()
            logger.info("Overview snapshot refreshed on schedule (saved_utc=%s)", saved)
        except Exception:
            logger.exception("Overview snapshot scheduled refresh failed")
            time.sleep(60)


def start_overview_snapshot_bootstrap() -> None:
    """
    Optionally run one ``refresh_overview_snapshot()`` shortly after startup (background).

    Controlled by ``OVERVIEW_SNAPSHOT_BOOTSTRAP`` (``missing`` | ``always`` | ``off``).
    """
    from config import OVERVIEW_SNAPSHOT_BOOTSTRAP

    mode = (OVERVIEW_SNAPSHOT_BOOTSTRAP or "missing").strip().lower()
    if mode in ("0", "off", "false", "no"):
        return
    if os.getenv("FLASK_DEBUG") == "1" and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return

    def run() -> None:
        time.sleep(3.0)
        path = snapshot_path()
        if mode in ("missing", "if-missing", ""):
            if path.exists():
                logger.info("Overview snapshot bootstrap skipped (file exists): %s", path)
                return
        elif mode not in ("always", "force", "yes", "1", "true"):
            logger.warning("Unknown OVERVIEW_SNAPSHOT_BOOTSTRAP %r; treating as missing", mode)
            if path.exists():
                return
        try:
            _, saved = refresh_overview_snapshot()
            logger.info("Overview snapshot bootstrap completed (saved_utc=%s)", saved)
        except Exception:
            logger.exception("Overview snapshot bootstrap failed")

    threading.Thread(target=run, name="overview-snapshot-bootstrap", daemon=True).start()
    logger.info("Overview snapshot bootstrap thread scheduled (mode=%s)", mode)


def start_daily_overview_scheduler() -> None:
    from config import (
        OVERVIEW_SCHEDULER_ENABLED,
        OVERVIEW_SNAPSHOT_HOUR,
        OVERVIEW_SNAPSHOT_TZ,
    )

    if not OVERVIEW_SCHEDULER_ENABLED:
        logger.info("Overview snapshot scheduler disabled (OVERVIEW_SCHEDULER_ENABLED)")
        return
    # Avoid double thread with Flask reloader parent process.
    if os.getenv("FLASK_DEBUG") == "1" and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return
    threading.Thread(target=_scheduler_loop, name="overview-snapshot-scheduler", daemon=True).start()
    logger.info(
        "Overview snapshot scheduler started (daily at %02d:00 %s)",
        int(OVERVIEW_SNAPSHOT_HOUR),
        OVERVIEW_SNAPSHOT_TZ,
    )
