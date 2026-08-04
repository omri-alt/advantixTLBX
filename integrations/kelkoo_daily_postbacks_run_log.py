"""
Append-only run log for the scheduled Kelkoo daily postback system.

Separate from resume state (``daily_conversion_postbacks_state.json``) and the
UI last-run snapshot (``daily_postbacks_last_run.json``).
"""
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
    from config import KELKOO_DAILY_POSTBACK_RUN_LOG_PATH

    p = Path(KELKOO_DAILY_POSTBACK_RUN_LOG_PATH)
    return p if p.is_absolute() else (Path(__file__).resolve().parents[1] / p)


def _utc_iso(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def append_run(entry: Dict[str, Any]) -> None:
    """Append one run entry; caps list length via ``KELKOO_DAILY_POSTBACK_RUN_LOG_MAX``."""
    row = dict(entry)
    row.setdefault("at_utc", _utc_iso())
    path = _log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with _lock:
        rows: List[Dict[str, Any]] = []
        if path.exists():
            try:
                raw = path.read_text(encoding="utf-8")
                rows = json.loads(raw) if raw.strip() else []
            except Exception as e:
                logger.warning("Kelkoo daily postback run log corrupt; resetting: %s", e)
                rows = []
        if not isinstance(rows, list):
            rows = []
        rows.append(row)
        from config import KELKOO_DAILY_POSTBACK_RUN_LOG_MAX

        cap = max(10, int(KELKOO_DAILY_POSTBACK_RUN_LOG_MAX))
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
