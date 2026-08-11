"""
Scheduled daily workflow v2 (09:00 Asia/Jerusalem by default).

Launches ``run_daily_workflow_v2.py`` via ``cli/run_workflow_job.py`` (same detached
pattern as the Control Center UI). Skips when a run is already in progress, or when
a successful run already started today in the scheduler timezone.
"""
from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_started = False
_run_lock = threading.Lock()

_WORKFLOW_KEY = "daily"
_WORKFLOW_TITLE = "Daily workflow (v2 staged)"
_SCRIPT = "run_daily_workflow_v2.py"


def _root() -> Path:
    return Path(__file__).resolve().parents[1]


def _runs_dir() -> Path:
    return _root() / "runtime" / "workflow_runs"


def _run_file() -> Path:
    return _runs_dir() / f"{_WORKFLOW_KEY}.json"


def _load_last_run() -> Dict[str, Any]:
    p = _run_file()
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_last_run(data: Dict[str, Any]) -> None:
    p = _run_file()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")
    tmp.replace(p)


def _pid_is_running(pid: Any) -> bool:
    try:
        p = int(pid)
    except (TypeError, ValueError):
        return False
    if p <= 0:
        return False
    try:
        if os.name == "nt":
            out = subprocess.run(
                ["tasklist", "/FI", f"PID eq {p}"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            text = ((out.stdout or "") + "\n" + (out.stderr or "")).lower()
            return f" {p} " in text or text.strip().endswith(str(p))
        os.kill(p, 0)
        return True
    except Exception:
        return False


def _parse_utc_iso(raw: str) -> Optional[datetime]:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _local_date_for(dt_utc: datetime, tz_name: str) -> str:
    tz = ZoneInfo(tz_name or "Asia/Jerusalem")
    return dt_utc.astimezone(tz).strftime("%Y-%m-%d")


def _seconds_until_local(hour: int, minute: int, tz_name: str) -> float:
    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return max(1.0, (target - now).total_seconds())


def _already_succeeded_today(tz_name: str) -> bool:
    last = _load_last_run()
    if str(last.get("status") or "").strip().lower() != "success":
        return False
    started = _parse_utc_iso(str(last.get("started_at_utc") or ""))
    if started is None:
        return False
    today = datetime.now(ZoneInfo(tz_name or "Asia/Jerusalem")).strftime("%Y-%m-%d")
    return _local_date_for(started, tz_name) == today


def run_daily_workflow_scheduled(
    *,
    triggered_by: str = "cron",
    extra_args: Optional[str] = None,
    force: bool = False,
) -> Dict[str, Any]:
    """
    Start daily workflow v2 in the background (non-blocking).

    Returns a status dict. Does not wait for the workflow to finish.
    """
    from config import (
        DAILY_WORKFLOW_SCHEDULER_EXTRA_ARGS,
        DAILY_WORKFLOW_SCHEDULER_TZ,
    )

    tz_name = DAILY_WORKFLOW_SCHEDULER_TZ or "Asia/Jerusalem"
    args_s = (
        extra_args
        if extra_args is not None
        else DAILY_WORKFLOW_SCHEDULER_EXTRA_ARGS
    )
    args_s = (args_s or "").strip()

    if not _run_lock.acquire(blocking=False):
        logger.warning("Daily workflow scheduler: another launch in progress; skipping")
        return {"ok": False, "error": "another_launch_in_progress", "triggered_by": triggered_by}

    try:
        last = _load_last_run()
        if (
            last
            and str(last.get("status") or "").strip().lower() == "running"
            and _pid_is_running(last.get("pid"))
        ):
            logger.info(
                "Daily workflow scheduler: already running (pid=%s); skipping",
                last.get("pid"),
            )
            return {
                "ok": False,
                "error": "already_running",
                "pid": last.get("pid"),
                "triggered_by": triggered_by,
            }

        if not force and _already_succeeded_today(tz_name):
            logger.info(
                "Daily workflow scheduler: successful run already today (%s); skipping",
                tz_name,
            )
            return {
                "ok": False,
                "error": "already_succeeded_today",
                "triggered_by": triggered_by,
            }

        root = _root()
        script = root / _SCRIPT
        if not script.is_file():
            return {
                "ok": False,
                "error": f"missing_script:{script}",
                "triggered_by": triggered_by,
            }

        wf_args = shlex.split(args_s, posix=(os.name != "nt")) if args_s else []
        cmd = [sys.executable, str(script), *wf_args]
        started_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        runner = root / "cli" / "run_workflow_job.py"
        launch_cmd = [
            sys.executable,
            str(runner),
            "--workflow-key",
            _WORKFLOW_KEY,
            "--workflow-title",
            _WORKFLOW_TITLE,
            "--runs-dir",
            str(_runs_dir()),
            "--cwd",
            str(root),
            "--started-at-utc",
            started_iso,
            "--",
            *cmd,
        ]
        popen_kwargs: Dict[str, Any] = {
            "cwd": str(root),
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
            "text": False,
        }
        if os.name == "nt":
            creationflags = 0
            creationflags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            creationflags |= getattr(subprocess, "DETACHED_PROCESS", 0)
            creationflags |= getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0)
            if creationflags:
                popen_kwargs["creationflags"] = creationflags
        else:
            popen_kwargs["start_new_session"] = True

        proc = subprocess.Popen(launch_cmd, **popen_kwargs)
        running = {
            "workflow_key": _WORKFLOW_KEY,
            "workflow_title": _WORKFLOW_TITLE,
            "status": "running",
            "exit_code": None,
            "started_at_utc": started_iso,
            "finished_at_utc": "",
            "duration_seconds": 0,
            "command": cmd,
            "args": wf_args,
            "pid": proc.pid,
            "triggered_by": triggered_by,
            "log": f"Scheduled start ({triggered_by}). Refresh workflow page for logs.",
        }
        _save_last_run(running)
        logger.info(
            "Daily workflow v2 launched (pid=%s, triggered_by=%s, args=%r)",
            proc.pid,
            triggered_by,
            args_s,
        )
        return {
            "ok": True,
            "pid": proc.pid,
            "started_at_utc": started_iso,
            "command": cmd,
            "triggered_by": triggered_by,
        }
    finally:
        _run_lock.release()


def _thread_loop() -> None:
    from config import (
        DAILY_WORKFLOW_SCHEDULER_HOUR_LOCAL,
        DAILY_WORKFLOW_SCHEDULER_MINUTE,
        DAILY_WORKFLOW_SCHEDULER_TZ,
    )

    while True:
        try:
            delay = _seconds_until_local(
                int(DAILY_WORKFLOW_SCHEDULER_HOUR_LOCAL),
                int(DAILY_WORKFLOW_SCHEDULER_MINUTE),
                DAILY_WORKFLOW_SCHEDULER_TZ,
            )
            logger.info(
                "Daily workflow scheduler: sleeping %.0fs until %02d:%02d %s",
                delay,
                int(DAILY_WORKFLOW_SCHEDULER_HOUR_LOCAL),
                int(DAILY_WORKFLOW_SCHEDULER_MINUTE),
                DAILY_WORKFLOW_SCHEDULER_TZ,
            )
            time.sleep(delay)
            run_daily_workflow_scheduled(triggered_by="cron")
        except Exception:
            logger.exception("Daily workflow scheduler loop failed")
            time.sleep(60)


def start_daily_workflow_scheduler() -> None:
    """Start thread fallback when AutoServer APScheduler is not owning the cron."""
    global _started
    from config import (
        AUTOSERVER_SCHEDULER_ENABLED,
        DAILY_WORKFLOW_SCHEDULER_ENABLED,
        DAILY_WORKFLOW_SCHEDULER_HOUR_LOCAL,
        DAILY_WORKFLOW_SCHEDULER_MINUTE,
        DAILY_WORKFLOW_SCHEDULER_TZ,
    )

    if not DAILY_WORKFLOW_SCHEDULER_ENABLED:
        logger.info("Daily workflow scheduler disabled (DAILY_WORKFLOW_SCHEDULER_ENABLED=0)")
        return
    if AUTOSERVER_SCHEDULER_ENABLED:
        logger.info(
            "Daily workflow uses APScheduler cron (AutoServer scheduler); thread loop skipped"
        )
        return
    if os.getenv("FLASK_DEBUG") == "1" and os.environ.get("WERKZEUG_RUN_MAIN") == "false":
        return
    if _started:
        return
    threading.Thread(target=_thread_loop, name="daily-workflow-scheduler", daemon=True).start()
    _started = True
    logger.info(
        "Daily workflow scheduler started (daily %02d:%02d %s)",
        int(DAILY_WORKFLOW_SCHEDULER_HOUR_LOCAL),
        int(DAILY_WORKFLOW_SCHEDULER_MINUTE),
        DAILY_WORKFLOW_SCHEDULER_TZ,
    )
