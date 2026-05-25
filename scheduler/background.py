"""
Start in-process background schedulers (AutoServer, Kelkoo late-sales prep, overview).

Under Gunicorn with multiple workers, a file lock ensures exactly one process runs
schedulers; the others serve HTTP only. Safe to call on every ``app`` import.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_scheduler_lock_fd: Optional[int] = None


def _scheduler_lock_path() -> Path:
    raw = (os.getenv("KLBLEND_SCHEDULER_LOCK_PATH") or "").strip()
    if raw:
        p = Path(raw)
        return p if p.is_absolute() else Path(__file__).resolve().parents[1] / p
    return Path(__file__).resolve().parents[1] / "runtime" / "scheduler_worker.lock"


def _try_acquire_scheduler_lock() -> bool:
    """Non-blocking flock; one live worker across Gunicorn processes (Linux)."""
    global _scheduler_lock_fd
    if _scheduler_lock_fd is not None:
        return True

    lock_path = _scheduler_lock_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        import fcntl

        fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            return False
        _scheduler_lock_fd = fd
        return True
    except ImportError:
        # Windows / no fcntl: single-process dev only.
        return True
    except Exception as e:
        logger.warning("Scheduler lock unavailable (%s); skipping background jobs", e)
        return False


def start_background_schedulers() -> None:
    """Start schedulers on this process if it holds the cluster-wide lock."""
    if not _try_acquire_scheduler_lock():
        logger.info(
            "Background schedulers not started on pid=%s (another worker holds the lock)",
            os.getpid(),
        )
        return

    from scheduler.autoserver_scheduler import start_autoserver_scheduler
    from scheduler.kelkoo_late_sales_scheduler import start_kelkoo_late_sales_scheduler
    from scheduler.yadore_sales_scheduler import start_yadore_sales_scheduler
    from integrations.overview_snapshot import (
        start_daily_overview_scheduler,
        start_overview_snapshot_bootstrap,
    )
    from scheduler.blend_cap_progress_scheduler import start_blend_cap_progress_scheduler
    from scheduler.blend_cpc_refresh_scheduler import start_blend_cpc_refresh_scheduler

    for label, fn in (
        ("AutoServer", start_autoserver_scheduler),
        ("Kelkoo late-sales prep", start_kelkoo_late_sales_scheduler),
        ("Yadore daily sales postbacks", start_yadore_sales_scheduler),
        ("Overview snapshot", start_daily_overview_scheduler),
        ("Overview bootstrap", start_overview_snapshot_bootstrap),
        ("Blend cap progress", start_blend_cap_progress_scheduler),
        ("Blend CPC refresh", start_blend_cpc_refresh_scheduler),
    ):
        try:
            fn()
        except Exception as e:
            logger.warning("%s scheduler did not start: %s", label, e)
