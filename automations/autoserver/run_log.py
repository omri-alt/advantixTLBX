from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_lock = threading.Lock()


def _log_path() -> Path:
    from config import AUTOSERVER_RUN_LOG_PATH

    p = Path(AUTOSERVER_RUN_LOG_PATH)
    return p if p.is_absolute() else (Path(__file__).resolve().parents[2] / p)


def _utc_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def append_run(
    *,
    automation: str,
    triggered_by: str,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    error: Optional[str],
) -> None:
    entry: Dict[str, Any] = {
        "automation": automation,
        "triggered_by": triggered_by,
        "started_at": _utc_iso(started_at),
        "finished_at": _utc_iso(finished_at),
        "status": status,
        "error": error,
    }
    path = _log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with _lock:
        rows: List[Dict[str, Any]] = []
        if path.exists():
            try:
                raw = path.read_text(encoding="utf-8")
                rows = json.loads(raw) if raw.strip() else []
            except Exception as e:
                logger.warning("autoserver run log corrupt; resetting: %s", e)
                rows = []
        if not isinstance(rows, list):
            rows = []
        rows.append(entry)
        from config import AUTOSERVER_RUN_LOG_MAX

        cap = max(10, int(AUTOSERVER_RUN_LOG_MAX))
        if len(rows) > cap:
            rows = rows[-cap:]
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)


def read_entries_newest_first(limit: int = 20) -> List[Dict[str, Any]]:
    path = _log_path()
    if not path.exists():
        return []
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(rows, list):
        return []
    rev = list(reversed(rows))
    if limit <= 0:
        return rev
    return rev[:limit]


def last_run_by_automation() -> Dict[str, Dict[str, Any]]:
    """Most recent log row per ``automation`` class name."""
    path = _log_path()
    if not path.exists():
        return {}
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(rows, list):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for row in reversed(rows):
        if not isinstance(row, dict):
            continue
        name = str(row.get("automation") or "")
        if name and name not in out:
            out[name] = row
    return out
