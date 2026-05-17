"""
Persisted overview dashboard data (``/api/overview`` reads from disk; rebuild is expensive).

- **Manual / scheduled rebuild:** ``queue_overview_refresh()`` (``POST /api/overview/refresh``) runs in a
  background thread so Gunicorn/proxy timeouts do not abort the job mid-flight.
- **Daily fire:** background thread sleeps until next ``OVERVIEW_SNAPSHOT_HOUR`` in ``OVERVIEW_SNAPSHOT_TZ``.

For multi-worker deployments, set ``OVERVIEW_SCHEDULER_ENABLED=0`` on all but one worker and use cron +
``python cli/refresh_overview_snapshot.py`` instead.
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

# Cross-process lock (Gunicorn workers). In-process guard avoids duplicate threads per worker.
_REFRESH_THREAD_LOCK = threading.Lock()
_REFRESH_THREAD: Optional[threading.Thread] = None

_STALE_RUNNING_SEC = 45 * 60
_STALE_LOCK_SEC = 45 * 60


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def snapshot_path() -> Path:
    from config import OVERVIEW_SNAPSHOT_PATH

    p = (OVERVIEW_SNAPSHOT_PATH or "").strip()
    if p:
        return Path(p)
    return ROOT / "runtime" / "overview_snapshot.json"


def refresh_state_path() -> Path:
    return snapshot_path().parent / "overview_refresh_state.json"


def refresh_lock_path() -> Path:
    return snapshot_path().parent / "overview_refresh.lock"


def read_refresh_state() -> Dict[str, Any]:
    path = refresh_state_path()
    if not path.is_file():
        return {"status": "idle"}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {"status": "idle"}
    except Exception as e:
        logger.warning("Overview refresh state read failed: %s", e)
        return {"status": "idle", "error": str(e)}


def write_refresh_state(state: Dict[str, Any]) -> None:
    path = refresh_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _parse_utc_ts(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.strptime(str(s).strip(), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _running_is_stale(state: Dict[str, Any]) -> bool:
    if state.get("status") != "running":
        return False
    started = _parse_utc_ts(state.get("started_utc"))
    if started is None:
        return True
    return (datetime.now(timezone.utc) - started).total_seconds() > _STALE_RUNNING_SEC


def _lock_is_stale() -> bool:
    path = refresh_lock_path()
    if not path.is_file():
        return False
    try:
        age = time.time() - path.stat().st_mtime
        return age > _STALE_LOCK_SEC
    except OSError:
        return True


def _try_acquire_file_lock() -> bool:
    path = refresh_lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file() and _lock_is_stale():
        try:
            path.unlink()
            logger.warning("Removed stale overview refresh lock: %s", path)
        except OSError as e:
            logger.warning("Could not remove stale overview lock: %s", e)
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            os.write(fd, f"{os.getpid()} {_utc_now()}\n".encode("utf-8"))
        finally:
            os.close(fd)
        return True
    except FileExistsError:
        return False


def _release_file_lock() -> None:
    path = refresh_lock_path()
    try:
        if path.is_file():
            path.unlink()
    except OSError as e:
        logger.warning("Could not release overview refresh lock: %s", e)


def refresh_overview_snapshot() -> Tuple[Dict[str, Any], str]:
    """Run ``build_overview_json`` and atomically write the snapshot file."""
    from integrations.overview import build_overview_json

    data = build_overview_json()
    saved_utc = _utc_now()
    path = snapshot_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    wrapped = {"saved_utc": saved_utc, "data": data}
    tmp.write_text(json.dumps(wrapped, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)
    return data, saved_utc


def _refresh_worker(*, reason: str) -> None:
    global _REFRESH_THREAD
    if not _try_acquire_file_lock():
        logger.info("Overview refresh skipped (%s): another process holds the lock", reason)
        with _REFRESH_THREAD_LOCK:
            _REFRESH_THREAD = None
        return
    started = _utc_now()
    write_refresh_state(
        {
            "status": "running",
            "reason": reason,
            "started_utc": started,
            "finished_utc": None,
            "saved_utc": None,
            "error": None,
        }
    )
    try:
        logger.info("Overview snapshot refresh started (reason=%s)", reason)
        _, saved = refresh_overview_snapshot()
        write_refresh_state(
            {
                "status": "done",
                "reason": reason,
                "started_utc": started,
                "finished_utc": _utc_now(),
                "saved_utc": saved,
                "error": None,
            }
        )
        logger.info("Overview snapshot refresh completed (saved_utc=%s)", saved)
    except Exception as e:
        logger.exception("Overview snapshot refresh failed (reason=%s)", reason)
        write_refresh_state(
            {
                "status": "error",
                "reason": reason,
                "started_utc": started,
                "finished_utc": _utc_now(),
                "saved_utc": None,
                "error": str(e),
            }
        )
    finally:
        _release_file_lock()
        with _REFRESH_THREAD_LOCK:
            _REFRESH_THREAD = None


def queue_overview_refresh(*, reason: str = "manual") -> Dict[str, Any]:
    """
    Start a background refresh if none is running (same worker + cross-worker lock).

    Returns current refresh state (``status`` may be ``running``, ``done``, ``error``, ``idle``).
    """
    global _REFRESH_THREAD

    state = read_refresh_state()
    if state.get("status") == "running" and not _running_is_stale(state):
        with _REFRESH_THREAD_LOCK:
            if _REFRESH_THREAD is not None and _REFRESH_THREAD.is_alive():
                return {**state, "queued": False}
        if refresh_lock_path().is_file() and not _lock_is_stale():
            return {**state, "queued": False}
        logger.warning("Overview refresh state=running but no active thread; treating as stale")
        write_refresh_state({**state, "status": "error", "error": "stale running state cleared"})

    with _REFRESH_THREAD_LOCK:
        if _REFRESH_THREAD is not None and _REFRESH_THREAD.is_alive():
            st = read_refresh_state()
            return {**st, "queued": False}

        started = _utc_now()
        pending = {
            "status": "running",
            "reason": reason,
            "started_utc": started,
            "finished_utc": None,
            "saved_utc": None,
            "error": None,
            "queued": True,
        }
        write_refresh_state(pending)
        _REFRESH_THREAD = threading.Thread(
            target=_refresh_worker,
            kwargs={"reason": reason},
            name="overview-snapshot-refresh",
            daemon=True,
        )
        _REFRESH_THREAD.start()
        return dict(pending)


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
            state = queue_overview_refresh(reason="schedule")
            logger.info("Overview snapshot scheduled refresh queued: %s", state.get("status"))
        except Exception:
            logger.exception("Overview snapshot scheduled refresh failed")
            time.sleep(60)


def start_overview_snapshot_bootstrap() -> None:
    """
    Optionally queue one refresh shortly after startup (background).

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
        queue_overview_refresh(reason="bootstrap")
        logger.info("Overview snapshot bootstrap refresh queued")

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
    if os.getenv("FLASK_DEBUG") == "1" and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return
    threading.Thread(target=_scheduler_loop, name="overview-snapshot-scheduler", daemon=True).start()
    logger.info(
        "Overview snapshot scheduler started (daily at %02d:00 %s)",
        int(OVERVIEW_SNAPSHOT_HOUR),
        OVERVIEW_SNAPSHOT_TZ,
    )
