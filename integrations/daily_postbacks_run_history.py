"""
Last-run snapshots per feed for the daily postbacks UI (dashboard + detail).

Separate from resume state in ``daily_conversion_postbacks_state.json``.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent


def last_run_path() -> Path:
    raw = (os.getenv("DAILY_POSTBACKS_LAST_RUN_PATH") or "").strip()
    if raw:
        return Path(raw)
    return ROOT / "runtime" / "daily_postbacks_last_run.json"


def load_last_runs() -> Dict[str, Any]:
    p = last_run_path()
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning("daily postbacks last-run read failed %s: %s", p, e)
        return {}


def record_last_run(
    target: str,
    report_date: str,
    *,
    dry_run: bool,
    ok: bool,
    summary: Dict[str, Any],
    batch_exit_code: int,
) -> None:
    p = last_run_path()
    data = load_last_runs()
    data[target.strip().lower()] = {
        "at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "report_date": report_date,
        "dry_run": bool(dry_run),
        "ok": bool(ok),
        "exit_code": int(batch_exit_code),
        "summary": summary,
    }
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
