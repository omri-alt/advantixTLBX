"""
Gunicorn: one worker runs background schedulers; others serve HTTP only.

Set ``KLBLEND_DEFER_SCHEDULERS=1`` so ``app.py`` does not start schedulers on every
worker import; ``post_fork`` acquires a file lock so exactly one live worker runs jobs
(even after worker restarts).
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

raw_env = [
    ("KLBLEND_DEFER_SCHEDULERS", "1"),
]


def post_fork(server, worker) -> None:
    os.environ["KLBLEND_SCHEDULER_WORKER"] = "0"
    lock_path = os.getenv("KLBLEND_SCHEDULER_LOCK_PATH", "").strip()
    if not lock_path:
        lock_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "runtime",
            "scheduler_worker.lock",
        )
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)

    try:
        import fcntl

        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            logger.info(
                "Background schedulers: worker pid=%s did not acquire lock (another worker is leader)",
                worker.pid,
            )
            return
        worker._klblend_scheduler_lock_fd = fd  # keep lock until worker exits
        os.environ["KLBLEND_SCHEDULER_WORKER"] = "1"
        from scheduler.background import start_background_schedulers

        start_background_schedulers()
        logger.info("Background schedulers started on worker pid=%s (lock %s)", worker.pid, lock_path)
    except ImportError:
        # Windows dev without fcntl: first post_fork only (legacy behaviour).
        if getattr(server, "_klblend_scheduler_assigned", False):
            return
        server._klblend_scheduler_assigned = True
        os.environ["KLBLEND_SCHEDULER_WORKER"] = "1"
        from scheduler.background import start_background_schedulers

        start_background_schedulers()
    except Exception:
        logger.exception("Failed to start background schedulers on worker pid=%s", worker.pid)
