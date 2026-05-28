#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _save_run(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")
    tmp.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Detached workflow executor for app UI runs.")
    parser.add_argument("--workflow-key", required=True)
    parser.add_argument("--workflow-title", required=True)
    parser.add_argument("--runs-dir", required=True)
    parser.add_argument("--cwd", required=True)
    parser.add_argument("--started-at-utc", default="")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="Command after --")
    ns = parser.parse_args()

    cmd = list(ns.command or [])
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if not cmd:
        return 2

    run_path = Path(ns.runs_dir) / f"{ns.workflow_key}.json"
    started = time.time()
    started_iso = (ns.started_at_utc or "").strip() or _utc_now_iso()

    try:
        popen_kwargs: dict[str, Any] = {
            "cwd": str(Path(ns.cwd)),
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "text": True,
        }
        if os.name == "nt":
            # Keep workflow alive even if starter process/job wrapper is recycled.
            creationflags = 0
            creationflags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            creationflags |= getattr(subprocess, "DETACHED_PROCESS", 0)
            creationflags |= getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0)
            if creationflags:
                popen_kwargs["creationflags"] = creationflags
        else:
            popen_kwargs["start_new_session"] = True
        child = subprocess.Popen(cmd, **popen_kwargs)
        out, err = child.communicate()
        rc = int(child.returncode or 0)
        finished = time.time()
        output = (out or "") + ("\n" if out and err else "") + (err or "")
        result = {
            "workflow_key": ns.workflow_key,
            "workflow_title": ns.workflow_title,
            "status": "success" if rc == 0 else "failed",
            "exit_code": rc,
            "started_at_utc": started_iso,
            "finished_at_utc": _utc_now_iso(),
            "duration_seconds": round(finished - started, 2),
            "command": cmd,
            "args": cmd[2:] if len(cmd) >= 3 else cmd[1:],
            "pid": None,
            "log": output.strip()[-20000:],
        }
        _save_run(run_path, result)
        return 0
    except Exception as e:
        failed = {
            "workflow_key": ns.workflow_key,
            "workflow_title": ns.workflow_title,
            "status": "failed",
            "exit_code": -1,
            "started_at_utc": started_iso,
            "finished_at_utc": _utc_now_iso(),
            "duration_seconds": 0,
            "command": cmd,
            "args": cmd[2:] if len(cmd) >= 3 else cmd[1:],
            "pid": None,
            "log": f"Detached workflow runner error: {e}",
        }
        _save_run(run_path, failed)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
